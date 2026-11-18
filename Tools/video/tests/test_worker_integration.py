"""Opt-in broker tests for FAIL-004 and EDIT-011.

Run against RabbitMQ 4 with PICTURAS_TEST_AMQP_URL set. Every test creates
randomly named resources; existing application queues are never consumed or
purged. FFmpeg/ffprobe and requirements-dev.txt are also required.
"""
import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import uuid

import pika
import pytest


def connect(url):
    parameters = pika.URLParameters(url)
    parameters.heartbeat = 0
    parameters.socket_timeout = 3
    parameters.blocked_connection_timeout = 5
    parameters.connection_attempts = 1
    return pika.BlockingConnection(parameters)


def receive(connection, channel, queue, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        delivery = channel.basic_get(queue=queue, auto_ack=False)
        if delivery[0] is not None:
            return delivery
        connection.process_data_events(time_limit=0.05)
    raise AssertionError(f"Timed out waiting for {queue}")


@pytest.fixture
def broker(monkeypatch):
    url = os.getenv("PICTURAS_TEST_AMQP_URL")
    if not url:
        pytest.skip("Set PICTURAS_TEST_AMQP_URL to run real RabbitMQ integration tests")
    try:
        control = connect(url)
    except (pika.exceptions.AMQPError, OSError) as error:
        pytest.fail(f"Requested RabbitMQ integration broker is unavailable ({type(error).__name__})")

    import topology

    spec = importlib.util.spec_from_file_location(
        "integration_video_worker", Path(__file__).parents[1] / "video.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    prefix = f"picturas-test-{uuid.uuid4().hex}"
    names = {
        "EXCHANGE": prefix,
        "DLX_EXCHANGE": f"{prefix}.dlx",
        "VIDEO_QUEUE": f"{prefix}.video",
        "VIDEO_ROUTING_KEY": f"{prefix}.video",
        "VIDEO_DLQ": f"{prefix}.dlq",
        "VIDEO_FAILED_EVENTS": f"{prefix}.failed",
        "PROJECT_QUEUE": f"{prefix}.project",
        "WS_QUEUE": f"{prefix}.ws",
        "LEGACY_QUEUE": f"{prefix}.legacy",
    }
    for name, value in names.items():
        if hasattr(topology, name):
            monkeypatch.setattr(topology, name, value)
        if hasattr(module, name):
            monkeypatch.setattr(module, name, value)

    connections = [control]

    def worker_connection():
        connection = connect(url)
        connections.append(connection)
        return connection

    monkeypatch.setattr(module.VideoWorker, "_connect", staticmethod(worker_connection))
    channel = control.channel()
    channel.exchange_declare(exchange=names["EXCHANGE"], exchange_type="direct", durable=True)
    channel.queue_declare(queue=names["LEGACY_QUEUE"], durable=True)
    for name in ("PROJECT_QUEUE", "WS_QUEUE"):
        channel.queue_declare(queue=names[name], durable=True)
        channel.queue_bind(queue=names[name], exchange=names["EXCHANGE"], routing_key=names[name])

    try:
        initial_worker = module.VideoWorker()
        initial_worker._connection.close()
        channel.confirm_delivery()
        yield SimpleNamespace(url=url, module=module, names=names,
                              connection=control, channel=channel)
    finally:
        for connection in connections[1:]:
            if connection.is_open:
                connection.close()
        if control.is_open:
            for name in ("VIDEO_QUEUE", "VIDEO_DLQ", "VIDEO_FAILED_EVENTS", "PROJECT_QUEUE", "WS_QUEUE", "LEGACY_QUEUE"):
                channel.queue_delete(queue=names[name])
            for name in ("EXCHANGE", "DLX_EXCHANGE"):
                channel.exchange_delete(exchange=names[name])
            control.close()


def publish(broker, source, output, identifier="request-1"):
    body = json.dumps({
        "messageId": identifier,
        "procedure": "video",
        "parameters": {
            "inputImageURI": str(source),
            "outputImageURI": str(output),
            "tools": [{"procedure": "brightness", "params": {"brightness": 1.1}}],
        },
    }).encode()
    broker.channel.basic_publish(
        exchange=broker.names["EXCHANGE"], routing_key=broker.names["VIDEO_ROUTING_KEY"],
        body=body, properties=pika.BasicProperties(delivery_mode=2), mandatory=True)
    return body


def dispatch(worker, queue):
    delivery = receive(worker._connection, worker._channel, queue)
    worker.callback(worker._channel, *delivery)
    return delivery


def read_json(broker, queue):
    method, properties, body = receive(broker.connection, broker.channel, queue)
    broker.channel.basic_ack(method.delivery_tag)
    return json.loads(body)


def test_transient_failure_retries_and_produces_one_valid_result(broker, monkeypatch,
                                                               sample_video, tmp_path):
    original = broker.module.process_video
    attempts = []

    def fail_once(*args, **kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise OSError("temporary storage failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(broker.module, "process_video", fail_once)
    output = tmp_path / "edited.mp4"
    publish(broker, sample_video, output)
    worker = broker.module.VideoWorker()
    first = dispatch(worker, broker.names["VIDEO_QUEUE"])
    assert not output.exists()
    second = dispatch(worker, broker.names["VIDEO_QUEUE"])

    assert not first[0].redelivered
    assert second[0].redelivered
    assert second[1].headers["x-delivery-count"] == 1
    reply = read_json(broker, broker.names["PROJECT_QUEUE"])
    assert reply["status"] == "success"
    assert reply["correlationId"] == "request-1"
    assert len(attempts) == 2 and output.is_file()
    assert reply["metadata"]["video"]["frames"] > 0
    assert not list(tmp_path.glob("*.part.mp4"))
    assert broker.channel.basic_get(broker.names["PROJECT_QUEUE"], auto_ack=True)[0] is None
    assert broker.channel.basic_get(broker.names["VIDEO_DLQ"], auto_ack=True)[0] is None


def test_repeated_failure_is_retained_in_dlq_and_notified_once(broker, monkeypatch, tmp_path):
    def fail(*args, **kwargs):
        raise OSError("persistent storage failure")

    monkeypatch.setattr(broker.module, "process_video", fail)
    original_body = publish(broker, "/unused.mp4", tmp_path / "unused.mp4")
    worker = broker.module.VideoWorker()
    dispatch(worker, broker.names["VIDEO_QUEUE"])
    dispatch(worker, broker.names["VIDEO_QUEUE"])
    failed_delivery = receive(worker._connection, worker._channel,
                              broker.names["VIDEO_FAILED_EVENTS"])
    worker.failed_callback(worker._channel, *failed_delivery)
    reply = read_json(broker, broker.names["PROJECT_QUEUE"])
    assert reply["status"] == "error"
    assert reply["correlationId"] == "request-1"
    assert reply["error"]["code"] == broker.module.CODES["max_retries_exceeded"]
    # Notifying the application consumes only the failure event. The original
    # failed request must remain available for inspection in the retained DLQ.
    method, properties, dead_body = receive(broker.connection, broker.channel,
                                            broker.names["VIDEO_DLQ"])
    assert dead_body == original_body
    assert properties.headers["x-death"][0]["reason"] == "delivery_limit"
    broker.channel.basic_ack(method.delivery_tag)
    assert broker.channel.basic_get(broker.names["PROJECT_QUEUE"], auto_ack=True)[0] is None
    assert broker.channel.basic_get(broker.names["VIDEO_QUEUE"], auto_ack=True)[0] is None


def crash_with_unacknowledged_delivery(url, queue):
    """Terminate a real consumer process without ack, nack or orderly AMQP close."""
    connection = connect(url)
    channel = connection.channel()
    receive(connection, channel, queue)
    os._exit(17)


def test_repeated_consumer_crashes_have_bounded_deliveries(broker, tmp_path):
    original_body = publish(broker, "/unused.mp4", tmp_path / "unused.mp4")
    context = multiprocessing.get_context("spawn")
    for _ in range(2):
        consumer = context.Process(target=crash_with_unacknowledged_delivery,
                                   args=(broker.url, broker.names["VIDEO_QUEUE"]))
        consumer.start()
        consumer.join(timeout=15)
        if consumer.is_alive():
            consumer.terminate()
            consumer.join(timeout=5)
            pytest.fail("Consumer did not receive a job before the deadline")
        assert consumer.exitcode == 17

    method, properties, dead_body = receive(broker.connection, broker.channel,
                                            broker.names["VIDEO_DLQ"])
    assert dead_body == original_body
    assert properties.headers["x-death"][0]["reason"] == "delivery_limit"
    broker.channel.basic_ack(method.delivery_tag)
    assert broker.channel.basic_get(broker.names["VIDEO_QUEUE"], auto_ack=True)[0] is None


def test_migration_preserves_pending_jobs_and_removes_legacy_routing(broker, monkeypatch,
                                                                   tmp_path):
    import migrate_queue

    for name in ("EXCHANGE", "VIDEO_QUEUE", "VIDEO_ROUTING_KEY", "LEGACY_QUEUE"):
        monkeypatch.setattr(migrate_queue, name, broker.names[name])
    broker.channel.queue_unbind(queue=broker.names["VIDEO_QUEUE"],
                                exchange=broker.names["EXCHANGE"],
                                routing_key=broker.names["VIDEO_ROUTING_KEY"])
    broker.channel.queue_bind(queue=broker.names["LEGACY_QUEUE"],
                              exchange=broker.names["EXCHANGE"],
                              routing_key=broker.names["VIDEO_ROUTING_KEY"])
    original_body = publish(broker, "/unused.mp4", tmp_path / "unused.mp4")
    preview = migrate_queue.migrate(broker.channel)
    assert preview["pending"] == 1 and preview["moved"] == 0
    with pytest.raises(ValueError, match="Stop video producers"):
        migrate_queue.migrate(broker.channel, apply=True)

    result = migrate_queue.migrate(broker.channel, apply=True, producers_stopped=True)
    assert result["moved"] == 1
    method, properties, moved_body = receive(broker.connection, broker.channel,
                                            broker.names["VIDEO_QUEUE"])
    assert moved_body == original_body and properties.delivery_mode == 2
    broker.channel.basic_ack(method.delivery_tag)
    assert broker.channel.basic_get(broker.names["LEGACY_QUEUE"], auto_ack=True)[0] is None

    next_body = publish(broker, "/unused.mp4", tmp_path / "next.mp4", "request-next")
    method, properties, routed_body = receive(broker.connection, broker.channel,
                                             broker.names["VIDEO_QUEUE"])
    assert routed_body == next_body
    broker.channel.basic_ack(method.delivery_tag)
    assert broker.channel.basic_get(broker.names["LEGACY_QUEUE"], auto_ack=True)[0] is None


def test_two_workers_process_videos_during_overlapping_intervals(broker, monkeypatch,
                                                               sample_video, tmp_path):
    original = broker.module.process_video
    both_started = threading.Barrier(2, timeout=10)
    intervals = {}

    def synchronized_processing(source, output, *args, **kwargs):
        started = time.monotonic()
        both_started.wait()
        result = original(source, output, *args, **kwargs)
        intervals[output] = (started, time.monotonic())
        return result

    monkeypatch.setattr(broker.module, "process_video", synchronized_processing)
    outputs = [tmp_path / f"video-{index}.mp4" for index in range(2)]
    for index, output in enumerate(outputs):
        publish(broker, sample_video, output, identifier=f"request-{index}")

    def consume_one():
        worker = broker.module.VideoWorker()
        try:
            dispatch(worker, broker.names["VIDEO_QUEUE"])
        finally:
            worker._connection.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(consume_one) for _ in range(2)]
        for future in futures:
            future.result(timeout=30)

    replies = [read_json(broker, broker.names["PROJECT_QUEUE"]) for _ in range(2)]
    assert all(reply["status"] == "success" for reply in replies)
    assert {reply["correlationId"] for reply in replies} == {"request-0", "request-1"}
    assert len(intervals) == 2
    assert max(start for start, end in intervals.values()) < min(end for start, end in intervals.values())
    assert all(output.is_file() for output in outputs)
    assert not list(tmp_path.glob("*.part.mp4"))
