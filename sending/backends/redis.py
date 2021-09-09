import aioredis

from ..base import AbstractPubSubManager, QueuedMessage


class RedisPubSubManager(AbstractPubSubManager):
    def __init__(self, dsn: str):
        super().__init__()
        self._dsn = dsn

    async def initialize(self, *args, **kwargs):
        self._redis = aioredis.from_url(self._dsn)
        self._redis_pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        return await super().initialize(*args, **kwargs)

    async def shutdown(self, now=False):
        await super().shutdown(now=now)
        await self._redis.close()
        self._redis = None
        self._redis_pubsub = None

    async def _create_topic_subscription(self, topic_name: str):
        await self._redis_pubsub.subscribe(topic_name)

    async def _cleanup_topic_subscription(self, topic_name: str):
        await self._redis_pubsub.unsubscribe(topic_name)

    async def _publish(self, message: QueuedMessage):
        await self._redis.publish(message.topic, message.contents)

    async def _poll(self):
        if self._redis_pubsub.subscribed:
            msg = await self._redis_pubsub.get_message(ignore_subscribe_messages=True)
            if msg is not None:
                self.schedule_for_delivery(msg["channel"], msg["data"])
