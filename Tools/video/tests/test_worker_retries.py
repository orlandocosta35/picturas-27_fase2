import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pika
import pytest


VIDEO_WORKER_PATH = Path(__file__).parents[1] / "video.py"
spec = importlib.util.spec_from_file_location("video_worker", VIDEO_WORKER_PATH)
video_worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(video_worker)


class FakeChannel:
    def __init__(self, publish_error=None):
        self.acks = []
        self.rejections = []
        self.published = []
        self.events = []
        self.publish_error = publish_error

    def basic_ack(self, delivery_tag):
        self.acks.append(delivery_tag)
        self.events.append("ack")

    def basic_reject(self, delivery_tag, requeue):
        self.rejections.append((delivery_tag, requeue))

    def basic_publish(self, exchange, routing_key, body, mandatory, properties):
        if self.publish_error:
            raise self.publish_error
        self.published.append({"exchange": exchange, "routing_key": routing_key,
                               "body": json.loads(body), "mandatory": mandatory,
                               "properties": properties})
        self.events.append("publish_confirmed")


def message():
    return SimpleNamespace(delivery_tag=17)


def properties(deliveries=0):
    return pika.BasicProperties(headers={"x-delivery-count": deliveries})


def request():
    return {
        "messageId": "request-1",
        "procedure": "video",
        "parameters": {
            "inputImageURI": "/input.mp4",
            "outputImageURI": "/output.mp4",
        },
    }


def body():
    return json.dumps(request()).encode()


def worker(channel):
    instance = object.__new__(video_worker.VideoWorker)
    instance._channel = channel
    return instance


def fail(error):
    def process(*args, **kwargs):
        raise error
    return process


@pytest.mark.parametrize("deliveries", [0, 1])
def test_unexpected_failure_is_rejected_for_broker_to_enforce_delivery_limit(monkeypatch,
                                                                           deliveries):
    channel = FakeChannel()
    monkeypatch.setattr(video_worker, "process_video", fail(OSError("storage unavailable")))

    worker(channel).callback(channel, message(), properties(deliveries), body())

    assert channel.acks == []
    assert channel.rejections == [(17, True)]
    assert channel.published == []


@pytest.mark.parametrize("error, code", [
    (video_worker.InvalidVideo("corrupt input"), "invalid_video"),
    (video_worker.InvalidTrim("trim end exceeds duration"), "invalid_trim"),
    (video_worker.InvalidSpeed("invalid speed"), "invalid_speed"),
    (video_worker.UnsupportedEffect("unknown effect"), "unsupported_effect"),
])
def test_domain_errors_are_reported_without_retry(monkeypatch, error, code):
    channel = FakeChannel()
    monkeypatch.setattr(video_worker, "process_video", fail(error))

    worker(channel).callback(channel, message(), properties(), body())

    assert channel.rejections == [] and channel.acks == [17]
    assert channel.events == ["publish_confirmed", "ack"]
    reply = channel.published[0]["body"]
    assert reply["correlationId"] == "request-1"
    assert reply["error"]["code"] == video_worker.CODES[code]


def test_success_is_published_persistently_before_acknowledgement(monkeypatch):
    channel = FakeChannel()
    monkeypatch.setattr(video_worker, "process_video", lambda *args, **kwargs: {"frames": 12})

    worker(channel).callback(channel, message(), properties(), body())

    assert channel.events == ["publish_confirmed", "ack"]
    assert channel.rejections == []
    publication = channel.published[0]
    assert publication["mandatory"] is True
    assert publication["properties"].delivery_mode == 2
    assert publication["properties"].content_type == "application/json"
    assert publication["properties"].message_id == publication["body"]["messageId"]
    assert publication["body"]["status"] == "success"
    assert publication["body"]["correlationId"] == "request-1"


def test_failed_success_publication_leaves_job_unacknowledged(monkeypatch):
    channel = FakeChannel(publish_error=pika.exceptions.UnroutableError([]))
    monkeypatch.setattr(video_worker, "process_video", lambda *args, **kwargs: {"frames": 12})

    with pytest.raises(pika.exceptions.UnroutableError):
        worker(channel).callback(channel, message(), properties(), body())

    assert channel.acks == [] and channel.rejections == []


def test_failed_domain_error_publication_leaves_job_unacknowledged(monkeypatch):
    channel = FakeChannel(publish_error=pika.exceptions.UnroutableError([]))
    monkeypatch.setattr(video_worker, "process_video", fail(video_worker.InvalidVideo("corrupt")))

    with pytest.raises(pika.exceptions.UnroutableError):
        worker(channel).callback(channel, message(), properties(), body())

    assert channel.acks == [] and channel.rejections == []


def test_connection_failure_during_processing_escapes_without_ack(monkeypatch):
    channel = FakeChannel()
    monkeypatch.setattr(video_worker, "process_video", fail(pika.exceptions.AMQPConnectionError()))

    with pytest.raises(pika.exceptions.AMQPConnectionError):
        worker(channel).callback(channel, message(), properties(), body())

    assert channel.acks == [] and channel.rejections == []


@pytest.mark.parametrize("invalid_body", [
    b"{invalid", bytes([255]), b"[]", b"null", b"{}",
    json.dumps({**request(), "messageId": 23}).encode(),
    json.dumps({**request(), "parameters": None}).encode(),
    json.dumps({**request(), "parameters": {**request()["parameters"], "tools": "brightness"}}).encode(),
    json.dumps({**request(), "parameters": {**request()["parameters"], "tools": [None]}}).encode(),
    json.dumps({**request(), "parameters": {**request()["parameters"], "tools": [{"procedure": "brightness", "params": []}]}}).encode(),
    json.dumps({**request(), "parameters": {**request()["parameters"], "progress": []}}).encode(),
])
def test_invalid_message_is_rejected_without_requeue(invalid_body):
    channel = FakeChannel()

    worker(channel).callback(channel, message(), properties(), invalid_body)

    assert channel.acks == []
    assert channel.rejections == [(17, False)]
    assert channel.published == []


def test_wrong_procedure_is_reported_without_processing(monkeypatch):
    channel = FakeChannel()
    monkeypatch.setattr(video_worker, "process_video", fail(AssertionError("must not process")))
    payload = {**request(), "procedure": "image"}

    worker(channel).callback(channel, message(), properties(), json.dumps(payload).encode())

    assert channel.rejections == [] and channel.acks == [17]
    assert channel.published[0]["body"]["error"]["code"] == video_worker.CODES["wrong_procedure"]


def test_terminal_failure_event_notifies_before_acknowledging_event():
    channel = FakeChannel()

    worker(channel).failed_callback(channel, message(), properties(), body())

    assert channel.events == ["publish_confirmed", "ack"]
    assert channel.rejections == []
    publication = channel.published[0]
    assert publication["routing_key"] == video_worker.PROJECT_QUEUE
    assert publication["body"]["correlationId"] == "request-1"
    assert publication["body"]["error"]["code"] == video_worker.CODES["max_retries_exceeded"]


def test_failed_terminal_notification_leaves_event_unacknowledged():
    channel = FakeChannel(publish_error=pika.exceptions.UnroutableError([]))

    with pytest.raises(pika.exceptions.UnroutableError):
        worker(channel).failed_callback(channel, message(), properties(), body())

    assert channel.acks == [] and channel.rejections == []


def test_malformed_failure_event_is_acknowledged_without_notification():
    channel = FakeChannel()

    worker(channel).failed_callback(channel, message(), properties(), b"null")

    assert channel.acks == [17] and channel.published == []


def test_main_reconnects_after_broker_failure_and_resumes_consuming(monkeypatch):
    attempts = []
    actions = []
    sleeps = []

    class RestartedWorker:
        def run(self):
            actions.append("consume")
            raise KeyboardInterrupt

        def close(self):
            actions.append("close")

    def connect_worker():
        attempts.append(1)
        if len(attempts) == 1:
            raise pika.exceptions.AMQPConnectionError()
        return RestartedWorker()

    monkeypatch.setattr(video_worker, "VideoWorker", connect_worker)
    monkeypatch.setattr(video_worker.time, "sleep", sleeps.append)

    with pytest.raises(KeyboardInterrupt):
        video_worker.main()

    assert len(attempts) == 2
    assert sleeps == [2]
    assert actions == ["consume", "close"]
