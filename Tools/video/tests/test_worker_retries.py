import json
import importlib.util
from pathlib import Path
from types import SimpleNamespace


VIDEO_WORKER_PATH = Path(__file__).parents[1] / "video.py"
spec = importlib.util.spec_from_file_location("video_worker", VIDEO_WORKER_PATH)
video_worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(video_worker)


class FakeChannel:
    def __init__(self):
        self.acks = []
        self.nacks = []
        self.published = []

    def basic_ack(self, delivery_tag):
        self.acks.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue):
        self.nacks.append((delivery_tag, requeue))

    def basic_publish(self, exchange, routing_key, body):
        self.published.append((exchange, routing_key, json.loads(body)))


def message(redelivered=False):
    return SimpleNamespace(delivery_tag=17, redelivered=redelivered)


def body():
    return json.dumps({
        "messageId": "request-1",
        "procedure": "video",
        "parameters": {
            "inputImageURI": "/input.mp4",
            "outputImageURI": "/output.mp4",
        },
    }).encode()


def worker(channel):
    instance = object.__new__(video_worker.VideoWorker)
    instance._channel = channel
    return instance


def test_unexpected_first_failure_is_requeued(monkeypatch):
    channel = FakeChannel()
    monkeypatch.setattr(
        video_worker,
        "process_video",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("temporary failure")),
    )

    worker(channel).callback(channel, message(), None, body())

    assert channel.acks == []
    assert channel.nacks == [(17, True)]
    assert channel.published == []


def test_repeated_failure_is_reported_and_sent_to_dlq(monkeypatch):
    channel = FakeChannel()
    monkeypatch.setattr(
        video_worker,
        "process_video",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("permanent failure")),
    )

    worker(channel).callback(channel, message(redelivered=True), None, body())

    assert channel.acks == []
    assert channel.nacks == [(17, False)]
    assert len(channel.published) == 1
    reply = channel.published[0]
    assert reply[1] == video_worker.PROJECT_QUEUE
    assert reply[2]["status"] == "error"
    assert reply[2]["error"]["code"] == video_worker.CODES["max_retries_exceeded"]


def test_invalid_message_is_rejected_without_requeue():
    channel = FakeChannel()

    worker(channel).callback(channel, message(), None, b"{invalid")

    assert channel.acks == []
    assert channel.nacks == [(17, False)]
    assert channel.published == []
