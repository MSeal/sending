"""This module defines an abstract PubSubManager class that must be used when
   implementing custom managers or other features."""
import abc
import asyncio
from collections import defaultdict, namedtuple
from functools import partial
from time import monotonic
from typing import Callable, Coroutine, Dict, Iterator, List, Set
from uuid import UUID, uuid4

from . import metrics
from .logging import logger
from .util import ensure_async, split_collection

QueuedMessage = namedtuple("QueuedMessage", ["topic", "contents", "session_id"])
Callback = namedtuple("Callback", ["method", "predicate"])

__not_in_a_session__ = object()


class AbstractPubSubManager(abc.ABC):
    """
    Manages the publish-subscribe workflow.

    This abstract class provides a common base class for a publish-subscribe
    workflow and the management of components in the workflow.

    The manager keeps track of message queues and workers that serve clients
    subscribed to a publisher's feed of messages.
    """

    def __init__(self):
        self.outbound_queue: asyncio.Queue[QueuedMessage] = None
        self.outbound_workers: List[asyncio.Task] = []

        self.inbound_queue: asyncio.Queue[QueuedMessage] = None
        self.inbound_workers: List[asyncio.Task] = []

        self.callback_delegation_workers = 1

        self.poll_workers: List[asyncio.Task] = []
        self.subscribed_topics_by_session: Dict[str, Set] = defaultdict(set)

        self.callbacks_by_id: Dict[UUID, Callback] = {}
        self.callback_ids_by_session: Dict[UUID, Set[UUID]] = defaultdict(set)

        self.inbound_message_hook: Coroutine = None
        self.outbound_message_hook: Coroutine = None

    async def initialize(
        self,
        *,
        queue_size=0,
        inbound_workers=1,
        outbound_workers=1,
        poll_workers=1,
        callback_delegation_workers=None,
    ):
        """Initialize a pub-sub channel, specifically its queues and workers."""
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
            self.poll_workers.append(asyncio.create_task(self._poll_loop()))

    async def shutdown(self, now=False):
        """Shut down a pub-sub channel and its related queues and workers"""
        if not now:
            await self._drain_queues()

        self.inbound_queue = None
        metrics.INBOUND_QUEUE_SIZE.set(0)

        self.outbound_queue = None
        metrics.OUTBOUND_QUEUE_SIZE.set(0)

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

        self.subscribed_topics_by_session.clear()
        metrics.SUBSCRIBED_TOPICS.set(0)

        self.callbacks_by_id.clear()
        self.callback_ids_by_session.clear()
        metrics.REGISTERED_CALLBACKS.set(0)

    async def _drain_queues(self):
        await self.inbound_queue.join()
        await self.outbound_queue.join()

    def send(self, topic_name: str, message):
        """Sends a message to a specific topic's queue."""
        self.outbound_queue.put_nowait(QueuedMessage(topic_name, message, None))
        metrics.OUTBOUND_QUEUE_SIZE.inc()

    async def subscribe_to_topic(self, topic_name: str, _session_id=__not_in_a_session__):
        """Subscribe to a publisher's topic"""
        if not self.is_subscribed_to_topic(topic_name):
            logger.info(f"Creating subscription to topic '{topic_name}'")
            await self._create_topic_subscription(topic_name)
            metrics.SUBSCRIBED_TOPICS.inc()

        logger.debug(f"Adding topic '{topic_name}' to session cache: {_session_id}")
        self.subscribed_topics_by_session[_session_id].add(topic_name)

    @abc.abstractmethod
    async def _create_topic_subscription(self, topic_name: str):
        pass

    async def unsubscribe_from_topic(self, topic_name: str, _session_id=__not_in_a_session__):
        """Unsubscribe from a specific topic's message feed."""
        if self.is_subscribed_to_topic(topic_name, _session_id):
            logger.debug(f"Removing topic '{topic_name}' from session cache: {_session_id}")
            self.subscribed_topics_by_session[_session_id].remove(topic_name)

        if not self.is_subscribed_to_topic(topic_name):
            logger.info(f"No more subscriptions to topic {topic_name}, cleaning up...")
            await self._cleanup_topic_subscription(topic_name)
            metrics.SUBSCRIBED_TOPICS.dec()

    @property
    def subscribed_topics(self) -> Set[str]:
        return set(
            [item for sublist in self.subscribed_topics_by_session.values() for item in sublist]
        )

    def is_subscribed_to_topic(self, topic_name: str, _session_id=None) -> bool:
        """Check if a client is subscribed to a specified topic."""
        if _session_id is not None:
            return topic_name in self.subscribed_topics_by_session[_session_id]
        else:
            return topic_name in self.subscribed_topics

    @abc.abstractmethod
    async def _cleanup_topic_subscription(self, topic_name: str):
        pass

    def register_callback(
        self, fn: Callable, on_predicate: Callable = None, _session_id=None
    ) -> Callable:
        """Register a subscriber callback with the publisher."""
        fn = ensure_async(fn)
        if on_predicate is not None:
            on_predicate = ensure_async(on_predicate)
        cb_id = str(uuid4())
        logger.debug(f"Registering callback: '{cb_id}'")
        self.callbacks_by_id[cb_id] = Callback(fn, on_predicate)

        if _session_id is not None:
            self.callback_ids_by_session[_session_id].add(cb_id)

        metrics.REGISTERED_CALLBACKS.inc()
        return partial(self._detach_callback, cb_id, _session_id)

    def _detach_callback(self, cb_id: UUID, _session_id: UUID):
        callback = self.callbacks_by_id.get(cb_id)
        if callback is not None:
            logger.info(f"Detaching callback: '{cb_id}'")
            del self.callbacks_by_id[cb_id]

            if _session_id is not None:
                self.callback_ids_by_session[_session_id].remove(cb_id)

            metrics.REGISTERED_CALLBACKS.dec()

    async def _outbound_worker(self):
        while True:
            message = await self.outbound_queue.get()
            try:
                if self.outbound_message_hook is not None:
                    coro = ensure_async(self.outbound_message_hook)
                    message = message._replace(contents=await coro(message.contents))
                await self._publish(message)
                metrics.OUTBOUND_MESSAGES_SENT.inc()
            except Exception:
                logger.exception("Uncaught exception found while publishing message")
                metrics.PUBLISH_MESSAGE_EXCEPTIONS.inc()
            finally:
                self.outbound_queue.task_done()
                metrics.OUTBOUND_QUEUE_SIZE.dec()

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

            try:
                if self.inbound_message_hook is not None:
                    coro = ensure_async(self.inbound_message_hook)
                    message = message._replace(contents=await coro(message.contents))

                if message.session_id is None:
                    callback_ids = list(self.callbacks_by_id.keys())
                else:
                    callback_ids = self.callback_ids_by_session[message.session_id]

                await asyncio.gather(
                    *[
                        self._delegate_to_callbacks(message, slice)
                        for slice in split_collection(
                            callback_ids, self.callback_delegation_workers
                        )
                    ]
                )
            except Exception:
                logger.exception("Uncaught exception found while processing inbound message")
            finally:
                self.inbound_queue.task_done()
                metrics.INBOUND_QUEUE_SIZE.dec()

    async def _delegate_to_callbacks(self, message: QueuedMessage, callback_ids: Iterator[UUID]):
        for id in callback_ids:
            cb = self.callbacks_by_id.get(id)
            if cb is not None:
                try:
                    if cb.predicate is None or await cb.predicate(message):
                        logger.debug(f"Delegating to callback: {id}")
                        enter = monotonic()
                        await cb.method(message.contents)
                        diff = monotonic() - enter
                        metrics.CALLBACK_DURATION.observe(diff)
                    else:
                        logger.debug(f"Skipping callback {id} because predicate returned False")
                except Exception:
                    logger.exception("Uncaught exception encountered while delegating to callback")
                    metrics.CALLBACK_EXCEPTIONS.inc()

    async def _poll_loop(self):
        while True:
            try:
                await self._poll()
            except Exception:
                logger.exception("Uncaught exception encountered while polling backend")
            finally:
                await asyncio.sleep(0)

    @abc.abstractmethod
    async def _poll(self):
        pass

    def schedule_for_delivery(self, topic, contents, _session_id=None):
        """Use contents to create and queue a message for the topic's feed."""
        message = QueuedMessage(topic, contents, _session_id)
        self.inbound_queue.put_nowait(message)
        metrics.INBOUND_QUEUE_SIZE.inc()
        metrics.INBOUND_MESSAGES_RECEIVED.inc()

    def get_session(self, use_isolated_session: bool = False):
        """Get the pub-sub manager's current session."""
        return PubSubSession(self) if use_isolated_session else PubSubSessionWithParent(self)


class PubSubSession:
    def __init__(self, parent: AbstractPubSubManager) -> None:
        self.id: str = str(uuid4())
        self.parent: AbstractPubSubManager = parent
        self._unregister_callbacks_by_id: Dict[str, Callable] = {}

    def send_to_callbacks(self, contents):
        """Send contents from a publisher to all subscribed callbacks."""

        # We don't currently use the topic name in the inbound worker
        # so setting this to None should be okay for now.
        self.parent.schedule_for_delivery(None, contents, self.id)

    @property
    def subscribed_topics(self) -> Set[str]:
        return self.parent.subscribed_topics_by_session[self.id]

    def is_subscribed_to_topic(self, topic_name: str) -> bool:
        """Check if a client is subscribed to a specified topic."""
        return topic_name in self.subscribed_topics

    async def subscribe_to_topic(self, topic_name: str):
        """Subscribe to the message feed for a topic."""
        return await self.parent.subscribe_to_topic(topic_name, self.id)

    async def unsubscribe_from_topic(self, topic_name: str):
        """Unsubscribe from the message feed for a topic."""
        return await self.parent.unsubscribe_from_topic(topic_name, self.id)

    def register_callback(self, fn: Callable, on_predicate: Callable = None):
        on_predicate = ensure_async(on_predicate) if on_predicate else ensure_async(lambda _: True)

        async def combined_predicates(message: QueuedMessage):
            topic_matches = message.topic is None or self.is_subscribed_to_topic(message.topic)
            return topic_matches and await on_predicate(message)

        unregister_callback_id = str(uuid4())
        unregister_callback = self.parent.register_callback(
            fn, on_predicate=combined_predicates, _session_id=self.id
        )
        """Register a subscriber callback with the publisher."""
        self._unregister_callbacks_by_id[unregister_callback_id] = unregister_callback
        return partial(self._detach_callback, unregister_callback_id)

    def _detach_callback(self, cb_id: str):
        # We do a second layer of ID-Callback caching here so that we can support
        # the detaching of callbacks mid-session but also so that we can pull the
        # session's cache of unregister callback methods and run them in batch
        # during cleanup.
        parent_detach_callback = self._unregister_callbacks_by_id.get(cb_id)
        if parent_detach_callback is not None:
            del self._unregister_callbacks_by_id[cb_id]
            return parent_detach_callback()

    async def stop(self):
        """Stop the processes and clear callbacks"""
        for cb in self._unregister_callbacks_by_id.values():
            cb()

        self._unregister_callbacks_by_id.clear()

        await asyncio.gather(
            *[self.unsubscribe_from_topic(topic_name) for topic_name in self.subscribed_topics]
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()


class PubSubSessionWithParent(PubSubSession):
    @property
    def subscribed_topics(self) -> Set[str]:
        all_session_topics = self.parent.subscribed_topics_by_session[__not_in_a_session__]
        return super().subscribed_topics | all_session_topics
