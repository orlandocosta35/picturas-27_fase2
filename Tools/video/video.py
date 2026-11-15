"""PictuRAS video microservice (RabbitMQ worker).

Receives the video and the whole effect pipeline in a single message,
processes it with video_processor and replies to the projects orchestrator on
project_queue. Progress is sent straight to the WebSocket gateway (ws_queue),
as a percentage.

Supports competing consumers (multiple replicas) and Dead Letter Queue (DLQ)
routing when a job crashes repeatedly or exceeds maximum delivery attempts.
"""
import datetime
import json
import os
import time
import uuid

import pika
import pytz
from pika.exchange_type import ExchangeType

import utils.env as env
from effects import UnsupportedEffect
from video_processor import InvalidSpeed, InvalidTrim, InvalidVideo, process_video

EXCHANGE = "picturas"
DLX_EXCHANGE = "picturas.dlx"
VIDEO_QUEUE = "video_queue"
VIDEO_DLQ = "video_dlq"
PROJECT_QUEUE = "project_queue"
WS_QUEUE = "ws_queue"
MICROSERVICE = "picturas-video-tool-ms"
MAX_REDELIVERIES = 1  # 1 initial attempt + 1 retry = 2 total attempts before DLQ

CODES = {
    "wrong_procedure": 2000,
    "error_processing": 2001,
    "unsupported_effect": 2002,
    "invalid_trim": 2003,
    "invalid_video": 2004,
    "invalid_speed": 2005,
    "max_retries_exceeded": 2006,
}


def now():
    return datetime.datetime.now(pytz.utc)


class VideoWorker:
    def __init__(self):
        self._connection = self._connect()
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=EXCHANGE,
                                       exchange_type=ExchangeType.direct, durable=True)
        self._channel.exchange_declare(exchange=DLX_EXCHANGE,
                                       exchange_type=ExchangeType.direct, durable=True)
        self._channel.queue_declare(queue=VIDEO_DLQ, durable=True)
        self._channel.queue_bind(queue=VIDEO_DLQ, exchange=DLX_EXCHANGE, routing_key=VIDEO_DLQ)

        self._channel.queue_declare(
            queue=VIDEO_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": DLX_EXCHANGE,
                "x-dead-letter-routing-key": VIDEO_DLQ,
            },
        )
        self._channel.queue_bind(queue=VIDEO_QUEUE, exchange=EXCHANGE, routing_key=VIDEO_QUEUE)
        # one video at a time per replica; parallelism comes from replicas
        self._channel.basic_qos(prefetch_count=1)

    @staticmethod
    def _connect():
        credentials = pika.PlainCredentials(env.RABBITMQ_USERNAME, env.RABBITMQ_PASSWORD)
        params = pika.ConnectionParameters(
            env.RABBITMQ_HOST, int(env.RABBITMQ_PORT), "/", credentials,
            # processing a video blocks this thread for minutes; without a
            # heartbeat the broker does not drop the connection midway
            heartbeat=0,
        )
        for attempt in range(30):
            try:
                return pika.BlockingConnection(params)
            except pika.exceptions.AMQPConnectionError:
                time.sleep(2)
        raise RuntimeError("Could not connect to RabbitMQ")

    def _publish(self, queue, msg):
        self._channel.basic_publish(exchange=EXCHANGE, routing_key=queue,
                                    body=json.dumps(msg))

    def _reply_success(self, msg_id, started, output_uri, video_info):
        output = {"type": video_info.get("output_type", "video"),
                  "imageURI": video_info.get("output_path", output_uri)}
        if output["type"] == "frames":
            output["frames"] = video_info["frames"]
        self._publish(PROJECT_QUEUE, {
            "messageId": f"video-{uuid.uuid4()}",
            "correlationId": msg_id,
            "timestamp": now().isoformat(),
            "status": "success",
            "output": output,
            "metadata": {
                "processingTime": (now() - started).total_seconds() * 1000,
                "microservice": MICROSERVICE,
                "video": video_info,
            },
        })

    def _reply_error(self, msg_id, started, code, message, input_uri):
        self._publish(PROJECT_QUEUE, {
            "messageId": f"video-{uuid.uuid4()}",
            "correlationId": msg_id,
            "timestamp": now().isoformat(),
            "status": "error",
            "error": {
                "code": code,
                "message": message,
                "msg": message,  # the orchestrator reads error.msg
                "details": {"inputFileURI": input_uri},
            },
            "metadata": {
                "processingTime": (now() - started).total_seconds() * 1000,
                "microservice": MICROSERVICE,
            },
        })

    def _progress(self, progress, preview, percent):
        if not progress or not progress.get("user"):
            return
        self._publish(WS_QUEUE, {
            "messageId": f"update-client-video-progress-{uuid.uuid4()}",
            "timestamp": now().isoformat(),
            "status": "success",
            "user": progress["user"],
            "projectId": progress.get("projectId"),
            "imgId": progress.get("imgId"),
            "preview": bool(preview),
            "percent": percent,
        })

    def callback(self, ch, method, properties, body):
        started = now()
        info = {}
        try:
            info = json.loads(body.decode())
            msg_id = info["messageId"]
            params = info["parameters"]
            input_uri = params["inputImageURI"]
            output_uri = params["outputImageURI"]
            tools = params.get("tools") or []
            preview = bool(params.get("preview"))
            progress = params.get("progress") or {}
        except (ValueError, KeyError) as e:
            print(f"[video] invalid message routed to DLQ: {e}", flush=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        # Check if message was already redelivered after a worker crash/OOM
        is_redelivered = bool(getattr(method, "redelivered", False))
        print(f"[video] msg={msg_id} redelivered={is_redelivered} preview={preview} "
              f"tools={[t.get('procedure') for t in tools]}", flush=True)

        if info.get("procedure") != "video":
            self._reply_error(msg_id, started, CODES["wrong_procedure"],
                              "The procedure received does not fit into this tool", input_uri)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            result = process_video(
                input_uri, output_uri, tools, preview=preview,
                progress_cb=lambda pct: self._progress(progress, preview, pct),
            )
            self._reply_success(msg_id, started, output_uri, result)
            print(f"[video] msg={msg_id} ok frames={result['frames']} "
                  f"t={(now() - started).total_seconds():.1f}s", flush=True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except (UnsupportedEffect, InvalidTrim, InvalidSpeed, InvalidVideo) as e:
            # Deterministic domain errors: report to user and acknowledge
            code_map = {
                UnsupportedEffect: CODES["unsupported_effect"],
                InvalidTrim: CODES["invalid_trim"],
                InvalidSpeed: CODES["invalid_speed"],
                InvalidVideo: CODES["invalid_video"],
            }
            self._reply_error(msg_id, started, code_map[type(e)], str(e), input_uri)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:  # noqa: BLE001
            print(f"[video] msg={msg_id} error (redelivered={is_redelivered}): {e}", flush=True)
            if is_redelivered:
                # Already retried once: isolate in DLQ and notify orchestrator/UI
                self._reply_error(
                    msg_id, started, CODES["max_retries_exceeded"],
                    f"Video processing failed after retry and was moved to DLQ: {e}",
                    input_uri,
                )
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            else:
                # First unexpected failure: requeue once for another healthy replica
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def run(self):
        self._channel.basic_consume(queue=VIDEO_QUEUE, on_message_callback=self.callback,
                                    auto_ack=False)
        print("[video] waiting for jobs on video_queue (DLQ enabled)", flush=True)
        self._channel.start_consuming()


if __name__ == "__main__":
    VideoWorker().run()