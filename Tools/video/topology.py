"""Versioned video queues: bounded retries, retained failures and notifications."""

EXCHANGE = "picturas"
DLX_EXCHANGE = "picturas.dlx"
VIDEO_QUEUE = "video_queue_v2"
# Keep the producer protocol stable while migrating the old classic queue.
VIDEO_ROUTING_KEY = "video_queue"
VIDEO_DLQ = "video_dlq"
VIDEO_FAILED_EVENTS = "video_failed_events"
MAX_REDELIVERIES = 1


def declare_topology(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=DLX_EXCHANGE, exchange_type="direct", durable=True)
    # One retained copy for inspection and another for terminal error reporting.
    for queue in (VIDEO_DLQ, VIDEO_FAILED_EVENTS):
        channel.queue_declare(queue=queue, durable=True)
        channel.queue_bind(queue=queue, exchange=DLX_EXCHANGE, routing_key=VIDEO_DLQ)
    channel.queue_declare(
        queue=VIDEO_QUEUE, durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-delivery-limit": MAX_REDELIVERIES,
            "x-dead-letter-exchange": DLX_EXCHANGE,
            "x-dead-letter-routing-key": VIDEO_DLQ,
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        },
    )
    channel.queue_bind(queue=VIDEO_QUEUE, exchange=EXCHANGE, routing_key=VIDEO_ROUTING_KEY)
