"""Move pending classic-queue video jobs to the versioned quorum queue.

Stop all video producers and consumers before applying. Source messages are
acknowledged only after the destination confirms their persistent publication.
An interruption between those operations can leave a duplicate, never an
intentionally discarded job. The old queue is retained for inspection.
"""
import argparse
import copy
import json
import os

import pika

from topology import EXCHANGE, VIDEO_QUEUE, VIDEO_ROUTING_KEY, declare_topology


LEGACY_QUEUE = "video_queue"


class LegacyQueueMissing(Exception):
    pass


def migrate(channel, *, apply=False, producers_stopped=False):
    """Inspect or migrate jobs on an existing AMQP channel.

    The caller owns the connection and must close it on failure, causing any
    unacknowledged source delivery to be requeued by RabbitMQ.
    """
    if apply and not producers_stopped:
        raise ValueError("Stop video producers and pass --producers-stopped before applying")
    try:
        source = channel.queue_declare(queue=LEGACY_QUEUE, passive=True).method
    except pika.exceptions.ChannelClosedByBroker as error:
        if error.reply_code == 404:
            raise LegacyQueueMissing from error
        raise
    if source.consumer_count:
        raise RuntimeError("Stop all legacy video consumers before migration")

    result = {"source": LEGACY_QUEUE, "destination": VIDEO_QUEUE,
              "pending": source.message_count, "moved": 0, "applied": apply}
    if not apply:
        return result

    declare_topology(channel)
    destination = channel.queue_declare(queue=VIDEO_QUEUE, passive=True).method
    if destination.consumer_count:
        raise RuntimeError("Stop all current video workers before migration")

    channel.confirm_delivery()
    channel.queue_unbind(queue=LEGACY_QUEUE, exchange=EXCHANGE,
                         routing_key=VIDEO_ROUTING_KEY)
    # Work only on the preflight snapshot. Producers must remain stopped.
    for _ in range(source.message_count):
        method, properties, body = channel.basic_get(queue=LEGACY_QUEUE, auto_ack=False)
        if method is None:
            raise RuntimeError("Legacy queue changed during migration; keep producers stopped and rerun")
        persistent = copy.copy(properties)
        persistent.delivery_mode = 2
        channel.basic_publish(exchange="", routing_key=VIDEO_QUEUE, body=body,
                              properties=persistent, mandatory=True)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        result["moved"] += 1
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Move jobs; otherwise only inspect")
    parser.add_argument("--producers-stopped", action="store_true",
                        help="Confirm projects and other video publishers are stopped")
    args = parser.parse_args()
    params = pika.URLParameters(os.environ.get(
        "RABBITMQ_URL", "amqp://user:password@rabbitmq:5672/%2F"))
    connection = pika.BlockingConnection(params)
    try:
        try:
            print(json.dumps(migrate(connection.channel(), apply=args.apply,
                                     producers_stopped=args.producers_stopped)))
        except LegacyQueueMissing:
            print("Legacy video_queue does not exist; no migration is needed.")
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
