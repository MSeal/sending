import abc
import asyncio
from collections import namedtuple
from functools import partial
from typing import Callable, Coroutine, Dict, Iterator, List, Set
from uuid import UUID, uuid4

from .logging import logger
from .util import ensure_async, split_collection

QueuedMessage = namedtuple("QueuedMessage", ["topic", "contents"])


# TODO: prometheus
# TODO: port over the callbacks-by-topic logic, add a sentinel value for
# "all topics" so we don't have to split them into separate methods like we do
# in Gate
# TODO: no more checking for messages we've seen before -- that can merged into
# a data validation hook, make an example!
# TODO: session contextmanager to ensure cleanup of callbacks and subscriptions
# TODO: cleanup subscriptions based on the number of subscribers. Distinguish
# between subscriptions made at the manager level and at a session level.
class AbstractPubSubManager(abc.ABC):
    def __init__(self):
        self.outbound_queue: asyncio.Queue[QueuedMessage] = None
        self.outbound_workers: List[asyncio.Task] = []

        self.inbound_queue: asyncio.Queue[QueuedMessage] = None
        self.inbound_workers: List[asyncio.Task] = []

        self.callback_delegation_workers = 1

        self.poll_workers: List[asyncio.Task] = []
        self.subscribed_topics: Set[str] = set()

        self.callbacks_by_id: Dict[UUID, Coroutine] = {}

    async def initialize(
        self,
        *,
        queue_size=0,
        inbound_workers=1,
        outbound_workers=1,
        poll_workers=1,
        callback_delegation_workers=None
    ):
        self.outbound_queue = asyncio.Queue(queue_size)
        self.inbound_queue = asyncio.Queue(queue_size)
        self.callback_delegation_workers = (
            callback_delegation_workers or self.callback_delegation_workers
        )

        for i in range(outbound_workers):
            self.outbound_workers.append(asyncio.create_task(self._outbound_worker()))

        for i in range(inbound_workers):
            self.inbound_workers.append(asyncio.create_task(self._inbound_worker()))

        for i in range(poll_workers):
            self.poll_workers.append(asyncio.create_task(self._poll()))

    async def shutdown(self, now=False):
        if not now:
            await self.inbound_queue.join()
            await self.outbound_queue.join()

        self.inbound_queue = None
        self.outbound_queue = None

        for worker in self.outbound_workers:
            worker.cancel()

        for worker in self.inbound_workers:
            worker.cancel()

        for worker in self.poll_workers:
            worker.cancel()

        await asyncio.gather(
            *self.outbound_workers,
            *self.inbound_workers,
            *self.poll_workers,
            return_exceptions=True,
        )

        self.outbound_workers.clear()
        self.inbound_workers.clear()
        self.poll_workers.clear()

    def send(self, topic_name: str, message):
        self.outbound_queue.put_nowait(QueuedMessage(topic_name, message))

    async def subscribe_to_topic(self, topic_name: str):
        await self._create_topic_subscription(topic_name)
        self.subscribed_topics.add(topic_name)

    @abc.abstractmethod
    async def _create_topic_subscription(self, topic_name: str):
        pass

    async def unsubscribe_from_topic(self, topic_name: str):
        await self._cleanup_topic_subscription(topic_name)
        self.subscribed_topics.remove(topic_name)

    @abc.abstractmethod
    async def _cleanup_topic_subscription(self, topic_name: str):
        pass

    def callback(self, fn: Callable) -> Callable:
        fn = ensure_async(fn)
        cb_id = uuid4()
        self.callbacks_by_id[cb_id] = fn
        return partial(self._detach_callback, cb_id)

    def _detach_callback(self, cb_id: UUID):
        if cb_id in self.callbacks_by_id:
            del self.callbacks_by_id[cb_id]

    async def _outbound_worker(self):
        while True:
            message = await self.outbound_queue.get()
            try:
                await self._publish(message)
            except Exception:
                logger.exception("Uncaught exception found while publishing message")
            self.outbound_queue.task_done()

    @abc.abstractmethod
    async def _publish(self, message: QueuedMessage):
        """The action needed to publish the message to the backend pubsub
        implementation.

        This will only be called by the outbound worker.
        """
        pass

    async def _inbound_worker(self):
        while True:
            message = await self.inbound_queue.get()
            contents = message.contents
            callback_ids = list(self.callbacks_by_id.keys())
            await asyncio.gather(
                *[
                    self._delegate_to_callbacks(contents, slice)
                    for slice in split_collection(callback_ids, self.callback_delegation_workers)
                ]
            )
            self.inbound_queue.task_done()

    async def _delegate_to_callbacks(self, contents, callback_ids: Iterator[UUID]):
        for id in callback_ids:
            cb = self.callbacks_by_id.get(id)
            if cb is not None:
                try:
                    await cb(contents)
                except Exception:
                    logger.exception("Uncaught exception encountered while delegating to callback")

    @abc.abstractmethod
    async def _poll(self):
        pass
