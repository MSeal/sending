"""Metrics for monitoring pub-sub operations"""
from prometheus_client import Counter, Gauge, Histogram

NAMESPACE = "sending"

INBOUND_QUEUE_SIZE = Gauge(
    "inbound_queue_size", "The current size of the inbound queue", namespace=NAMESPACE
)
OUTBOUND_QUEUE_SIZE = Gauge(
    "outbound_queue_size", "The current size of the outbound queue", namespace=NAMESPACE
)

INBOUND_MESSAGES_RECEIVED = Counter(
    "inbound_messages", "Total number of inbound messages received", namespace=NAMESPACE
)
OUTBOUND_MESSAGES_SENT = Counter(
    "outbound_messages", "Total number of outbound messages sent", namespace=NAMESPACE
)

PUBLISH_MESSAGE_EXCEPTIONS = Counter(
    "publish_message_failures",
    "Total number of uncaught exceptions from message publication",
)

SUBSCRIBED_TOPICS = Gauge(
    "subscribed_topics", "The current number of subscribed topics", namespace=NAMESPACE
)

REGISTERED_CALLBACKS = Gauge(
    "registered_callbacks",
    "The current number of registered callbacks",
    namespace=NAMESPACE,
)
CALLBACK_DURATION = Histogram(
    "callback_duration_seconds",
    "Time taken for a callback to resolve",
    namespace=NAMESPACE,
)
CALLBACK_EXCEPTIONS = Counter(
    "callback_exceptions",
    "The number of uncaught callback exceptions",
    namespace=NAMESPACE,
)
