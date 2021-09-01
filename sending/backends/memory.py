import asyncio

from ..base import AbstractPubSubManager, QueuedMessage


class InMemoryPubSubManager(AbstractPubSubManager):
    def __init__(self):
        super().__init__()
        self.message_queue: asyncio.Queue[QueuedMessage] = None

    async def initialize(self, *args, **kwargs):
        queue_size = kwargs.get("queue_size", 0)
        self.message_queue = asyncio.Queue(queue_size)
        return await super().initialize(*args, **kwargs)

    async def shutdown(self, now=False):
        await super().shutdown(now=now)
        self.message_queue = None

    async def _drain_queues(self):
        await self.outbound_queue.join()
        await self.message_queue.join()
        await self.inbound_queue.join()

    async def _create_topic_subscription(self, topic_name: str):
        # No external action needs to be taken
        pass

    async def _cleanup_topic_subscription(self, topic_name: str):
        # No external action needs to be taken
        pass

    async def _publish(self, message: QueuedMessage):
        self.message_queue.put_nowait(message)

    async def _poll(self):
        msg = await self.message_queue.get()
        if msg.topic in self.subscribed_topics:
            self.schedule_for_delivery(msg.topic, msg.contents, msg.session_id)
        self.message_queue.task_done()
