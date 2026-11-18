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
import time
import uuid

import pika
import pytz

import utils.env as env
from effects import UnsupportedEffect
from video_processor import InvalidSpeed, InvalidTrim, InvalidVideo, process_video

from topology import (EXCHANGE, VIDEO_QUEUE, VIDEO_FAILED_EVENTS, MAX_REDELIVERIES,
                      declare_topology)

PROJECT_QUEUE = "project_queue"
WS_QUEUE = "ws_queue"
MICROSERVICE = "picturas-video-tool-ms"

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
        try:
            self._channel = self._connection.channel()
            declare_topology(self._channel)
            self._channel.confirm_delivery()
            # One video at a time per replica; parallelism comes from replicas.
            self._channel.basic_qos(prefetch_count=1)
        except Exception:
            if self._connection.is_open:
                self._connection.close()
            raise

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
                                    body=json.dumps(msg), mandatory=True,
                                    properties=pika.BasicProperties(
                                        delivery_mode=2, content_type="application/json",
                                        message_id=msg.get("messageId"),
                                    ))

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

    @staticmethod
    def _request(body):
        info = json.loads(body.decode())
        if not isinstance(info, dict) or not isinstance(info.get("parameters"), dict):
            raise ValueError("Expected a video request with parameters")
        params = info["parameters"]
        for name, value in (("messageId", info.get("messageId")),
                            ("inputImageURI", params.get("inputImageURI")),
                            ("outputImageURI", params.get("outputImageURI"))):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Missing or invalid {name}")
        tools = params.get("tools", [])
        if not isinstance(tools, list) or any(
            not isinstance(tool, dict) or not isinstance(tool.get("procedure"), str)
            or not isinstance(tool.get("params", {}), dict) for tool in tools
        ):
            raise ValueError("Expected a list of tools with procedure and params")
        if not isinstance(params.get("progress", {}), dict):
            raise ValueError("Expected progress metadata")
        return info, params

    def callback(self, ch, method, properties, body):
        started = now()
        try:
            info, params = self._request(body)
        except (ValueError, UnicodeError) as e:
            print(f"[video] invalid message routed to DLQ: {e}", flush=True)
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return

        msg_id = info["messageId"]
        input_uri = params["inputImageURI"]
        output_uri = params["outputImageURI"]
        tools = params.get("tools", [])
        preview = bool(params.get("preview"))
        progress = params.get("progress", {})
        deliveries = (getattr(properties, "headers", None) or {}).get("x-delivery-count", 0)
        print(f"[video] msg={msg_id} retry={deliveries}/{MAX_REDELIVERIES} "
              f"preview={preview} tools={[t['procedure'] for t in tools]}", flush=True)

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
        except (UnsupportedEffect, InvalidTrim, InvalidSpeed, InvalidVideo) as e:
            # Deterministic errors need user correction, not retries.
            code_map = {
                UnsupportedEffect: CODES["unsupported_effect"],
                InvalidTrim: CODES["invalid_trim"],
                InvalidSpeed: CODES["invalid_speed"],
                InvalidVideo: CODES["invalid_video"],
            }
            self._reply_error(msg_id, started, code_map[type(e)], str(e), input_uri)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        except pika.exceptions.AMQPError:
            # Reconnect without ack: the broker also counts connection failures.
            raise
        except Exception as e:
            print(f"[video] msg={msg_id} processing failed: {e}", flush=True)
            # Quorum delivery counts survive consumer crashes/OOM. Unlike a
            # boolean redelivered flag they enforce a bound across replicas.
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)
            return

        # Publication failures escape to the reconnect loop. Never acknowledge a
        # job until its persistent, mandatory reply has been broker-confirmed.
        self._reply_success(msg_id, started, output_uri, result)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[video] msg={msg_id} ok frames={result['frames']} "
              f"t={(now() - started).total_seconds():.1f}s", flush=True)

    def failed_callback(self, ch, method, properties, body):
        """Notify terminal failure without consuming the retained DLQ copy."""
        try:
            info, params = self._request(body)
        except (ValueError, UnicodeError):
            # A malformed request may not have an ID to correlate. Its original
            # body stays in the DLQ for inspection, rather than killing workers.
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        self._reply_error(
            info["messageId"], now(), CODES["max_retries_exceeded"],
            "Video processing could not be completed after automatic retries. "
            "The request has been retained for investigation.",
            params["inputImageURI"],
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        self._channel.basic_consume(queue=VIDEO_QUEUE, on_message_callback=self.callback,
                                    auto_ack=False)
        self._channel.basic_consume(queue=VIDEO_FAILED_EVENTS,
                                    on_message_callback=self.failed_callback, auto_ack=False)
        print(f"[video] waiting for jobs on {VIDEO_QUEUE} (bounded retries enabled)", flush=True)
        self._channel.start_consuming()

    def close(self):
        if self._connection.is_open:
            self._connection.close()


def main():
    while True:
        worker = None
        try:
            worker = VideoWorker()
            worker.run()
        except (pika.exceptions.AMQPError, OSError, RuntimeError) as e:
            print(f"[video] connection unavailable, reconnecting: {e}", flush=True)
            time.sleep(2)
        finally:
            if worker is not None:
                try:
                    worker.close()
                except pika.exceptions.AMQPError:
                    pass


if __name__ == "__main__":
    main()
