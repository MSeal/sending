"""Publish Subscribe Managers and PubSub sessions.

This module defines an abstract `AbstractPubSubManager` class that must be
subclassed when implementing custom managers or other features.

This module also implements a `DetachedPubSubSession` that receives messages
only for topics which it has a subscription.

The module also implements a more robust `PubSubSession`, a subclass of
`DetachedPubSubSession, which receives messages from topics that it or its
parent have a subscription.
"""
import abc
import asyncio
import enum
import traceback
from collections import defaultdict, namedtuple
from functools import partial, wraps
from typing import Callable, Dict, List, Optional, Set
from uuid import UUID, uuid4

from .logging import logger
from .util import ensure_async

QueuedMessage = namedtuple("QueuedMessage", ["topic", "contents", "session_id"])
Callback = namedtuple("Callback", ["method", "predicate", "qualname"])

__not_in_a_session__ = object()

# Reserved name for system events sent by the library
SYSTEM_TOPIC = "__sending__"


class SystemEvents(enum.Enum):
    """System events related to the pub/sub manager

    We've forcibly disconnected from the pub/sub server. This is for situations
    where the underlying backend has forced a disconnect without the user
    asking. ZMQ `MAXMSGSIZE` cycling connections is a good example.
    """

    FORCED_DISCONNECT = enum.auto()


class AbstractPubSubManager(abc.ABC):
    """A manager for the publish-subscribe workflow.

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

        self.poll_workers: List[asyncio.Task] = []
        self.subscribed_topics_by_session: Dict[str, Set] = defaultdict(set)

        self.callbacks_by_id: Dict[UUID, Callback] = {}
        self.callback_ids_by_session: Dict[UUID, Set[UUID]] = defaultdict(set)

        # Allow these hooks to be defined within the class or attached to an instance
        if not hasattr(self, "inbound_message_hook"):
            # Called by _inbound_worker when picking up a message from inbound queue
            # Primarily used for deserializing messages from the wire
            # Will get one argument: incoming "raw" message content over the wire
            self.inbound_message_hook: Optional[Callable] = None
        if not hasattr(self, "outbound_message_hook"):
            # Called by _outbound_worker before pushing a message to _publish
            # Primarily used for serializing messages going out over the wire
            # Will get one argument, the QueuedMessage.contents coming out of .send()
            self.outbound_message_hook: Optional[Callable] = None
        if not hasattr(self, "context_hook"):
            # Called at .initialize() and then within the while True loop for
            # each worker. Should be used to set structlog.contextvars.bind_contextvars.
            # Takes no arguments
            self.context_hook: Optional[Callable] = None
        if not hasattr(self, "callback_hook"):
            # Called when the inbound worker is about to pass a QueuedMessage.contents
            # into a callback. Primarily used for logging the QueuedMessage.topic as
            # contextvars in the callback logging. Can be removed if we begin passing
            # kwargs / full QueuedMessage to callbacks.
            # Takses two arguments: the QueuedMessage and the Callback
            self.callback_hook: Optional[Callable] = None

    async def initialize(
        self,
        *,
        queue_size=0,
        inbound_workers=1,
        outbound_workers=1,
        poll_workers=1,
        enable_polling=True,
    ):
        """Initialize a pub-sub channel, specifically its queues and workers.

        `enable_polling` if set to True, the default, will start the poll
        workers. If `enable_polling` is False, it will only start the inbound
        and outbound workers and not start the poll worker. The False setting
        is useful if you're writing tests and don't want a connection to
        external IO to be started.

        ```
        mgr = SomeBackend()
        publish = mocker.patch.object(mgr, "_publish")

        @mgr.callback(on_topic="test-topic")
        def echo(msg: str):
            mgr.send(topic_name="test-topic", message=msg)

        await mgr.initialize(enable_polling=False)
        mgr.schedule_for_delivery(topic="test-topic", contents="echo test")
        await asyncio.sleep(0.01)
        publish.assert_called_once_with(
            QueuedMessage(topic="test-topic, contents="echo test", session_id=None)
        )
        ```
        """
        if self.context_hook:
            await self.context_hook()
        self.outbound_queue = asyncio.Queue(queue_size)
        self.inbound_queue = asyncio.Queue(queue_size)

        for i in range(outbound_workers):
            self.outbound_workers.append(asyncio.create_task(self._outbound_worker()))

        for i in range(inbound_workers):
            self.inbound_workers.append(asyncio.create_task(self._inbound_worker()))

        if enable_polling:
            for i in range(poll_workers):
                self.poll_workers.append(asyncio.create_task(self._poll_loop()))

    async def shutdown(self, now=False):
        """Shut down a pub-sub channel and its related queues and workers.

        The `now` parameter will drain queues gracefully if set to the default
        False. If set to True, the queues are cleared and set to None.
        """
        if not now:
            await self._drain_queues()

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
        self.subscribed_topics_by_session.clear()
        self.callbacks_by_id.clear()
        self.callback_ids_by_session.clear()

    async def _drain_queues(self):
        """Private method which waits for queues to clear."""
        await self.inbound_queue.join()
        await self.outbound_queue.join()

    def send(self, topic_name: str, message):
        """Sends a message to a specific topic's queue."""
        self.outbound_queue.put_nowait(QueuedMessage(topic_name, message, None))

    async def subscribe_to_topic(self, topic_name: str, _session_id=__not_in_a_session__):
        """Subscribe to a publisher's topic"""
        if not self.is_subscribed_to_topic(topic_name):
            logger.info(f"Creating subscription to topic '{topic_name}'")
            await self._create_topic_subscription(topic_name)

        if _session_id is __not_in_a_session__:
            logger.debug(f"Adding topic '{topic_name}' for all sessions")
        else:
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
        self,
        fn: Callable,
        *,
        on_topic: str = None,
        on_predicate: Callable = None,
        on_system_event: SystemEvents = None,
        _session_id=None,
    ) -> Callable:
        """Register a subscriber callback with the publisher."""

        if on_system_event:
            on_topic = SYSTEM_TOPIC

        async def predicate(topic, message, system_event=None):
            if topic == SYSTEM_TOPIC and on_topic != SYSTEM_TOPIC:
                # You have to explicitly opt-in to receiving system events
                return False
            if on_topic and on_topic != topic:
                return False
            if on_predicate and not await ensure_async(on_predicate)(topic, message):
                return False
            if on_system_event and on_system_event != system_event:
                return False
            return True

        fn = ensure_async(fn)
        cb_id = str(uuid4())

        if hasattr(fn, "__qualname__"):
            qualname = fn.__qualname__
        elif hasattr(fn, "__repr__"):
            qualname = fn.__repr__
        else:
            qualname = cb_id

        logger.debug(f"Registering callback: '{qualname}'")
        self.callbacks_by_id[cb_id] = Callback(fn, predicate, qualname)

        if _session_id is not None:
            self.callback_ids_by_session[_session_id].add(cb_id)

        return partial(self._detach_callback, cb_id, _session_id)

    def callback(self, on_topic=None, on_predicate=None, on_system_event=None) -> Callable:
        def decorator(fn):
            self.register_callback(
                fn, on_topic=on_topic, on_predicate=on_predicate, on_system_event=on_system_event
            )

            @wraps(fn)
            def wrapped_fn(*args, **kwargs):
                return fn(*args, **kwargs)

            return wrapped_fn

        return decorator

    def _detach_callback(self, cb_id: UUID, _session_id: UUID):
        callback = self.callbacks_by_id.get(cb_id)
        if callback is not None:
            logger.info(f"Detaching callback: '{callback.qualname}'")
            del self.callbacks_by_id[cb_id]

            if _session_id is not None:
                self.callback_ids_by_session[_session_id].remove(cb_id)

    async def _outbound_worker(self):
        while True:
            message = await self.outbound_queue.get()
            if self.context_hook:
                await self.context_hook()
            try:
                if self.outbound_message_hook is not None:
                    coro = ensure_async(self.outbound_message_hook)
                    message = message._replace(contents=await coro(message.contents))
                logger.debug(f"Sending message to topic: {message.topic}")
                await self._publish(message)
            except Exception as e:
                tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
                logger.exception(
                    "Uncaught exception found while publishing message", exc_info="".join(tb_str)
                )
            finally:
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
            if self.context_hook:
                await self.context_hook()
            try:
                if self.inbound_message_hook is not None and message.topic is not SYSTEM_TOPIC:
                    coro = ensure_async(self.inbound_message_hook)
                    message = message._replace(contents=await coro(message.contents))

                if message.session_id is None:
                    callback_ids = list(self.callbacks_by_id.keys())
                else:
                    callback_ids = self.callback_ids_by_session[message.session_id]

                await asyncio.gather(
                    *[self._delegate_to_callback(message, cb_id) for cb_id in callback_ids]
                )
            except Exception as e:
                tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
                logger.exception(
                    "Uncaught exception found while processing inbound message",
                    exc_info="".join(tb_str),
                )
            finally:
                self.inbound_queue.task_done()

    async def _delegate_to_callback(self, message: QueuedMessage, callback_id: UUID):
        cb = self.callbacks_by_id.get(callback_id)
        if cb is not None:
            try:
                system_event = None
                if message.topic == SYSTEM_TOPIC:
                    system_event = message.contents.get("event")
                if cb.predicate is None or await cb.predicate(
                    message.topic, message.contents, system_event=system_event
                ):
                    logger.debug(f"Delegating to callback: '{cb.qualname}'")
                    # TODO(nick): I would love to have a set of kwargs that are passed around
                    # for callbacks + predicates that you opt-in to. That would be a bit easier
                    # to document and access.
                    if self.callback_hook:
                        await self.callback_hook(message, cb)
                    await cb.method(message.contents)
                else:
                    logger.debug(
                        f"Skipping callback '{cb.qualname}' because predicate returned False"
                    )
            except Exception as e:
                tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
                logger.exception(
                    "Uncaught exception encountered while delegating to callback",
                    exc_info="".join(tb_str),
                )

    async def _poll_loop(self):
        while True:
            if self.context_hook:
                await self.context_hook()
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
        logger.debug(f"Scheduling message for delivery on topic: {topic}")
        message = QueuedMessage(topic, contents, _session_id)
        self.inbound_queue.put_nowait(message)

    def get_detached_session(self):
        """Get a new session for callbacks and subscriptions that won't receive
        global messages from this parent.
        """
        return DetachedPubSubSession(self)

    def get_session(self):
        """Get a new session for callbacks and subscriptions."""
        return PubSubSession(self)

    def _emit_system_event(self, topic: str, event: SystemEvents):
        logger.debug(f"Emitting system event: {event}")
        self.schedule_for_delivery(SYSTEM_TOPIC, {"event": event, "topic": topic})


class DetachedPubSubSession:
    """A session that receives messages from subscribed topics.

    It still relies on the parent PubSubManager as the centralized queuing
    mechanism for processing inbound and outbound messages. It also ensures
    total isolation for what messages get passed down to the session's callbacks
    (detached from its parent's subscriptions). This is helpful when a separate
    polling process is needed for each session.
    """

    def __init__(self, parent: AbstractPubSubManager) -> None:
        self.id: str = str(uuid4())
        self.parent: AbstractPubSubManager = parent
        self._unregister_callbacks_by_id: Dict[str, Callable] = {}

    def send_to_callbacks(self, contents, topic_name=None):
        """Send contents from a publisher to all subscribed callbacks."""
        self.parent.schedule_for_delivery(topic_name, contents, self.id)

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

    def register_callback(
        self, fn: Callable, *, on_topic: str = None, on_predicate: Callable = None
    ):
        async def combined_predicates(topic, contents):
            if topic and not self.is_subscribed_to_topic(topic):
                return False
            if on_predicate and not await ensure_async(on_predicate)(topic, contents):
                return False
            return True

        unregister_callback_id = str(uuid4())
        unregister_callback = self.parent.register_callback(
            fn, on_topic=on_topic, on_predicate=combined_predicates, _session_id=self.id
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


class PubSubSession(DetachedPubSubSession):
    """A holder for callbacks and topic subscriptions.

    Also receives messages from topics subscribed to by the parent manager.
    This is helpful if you have a global topic that all sessions should subscribe to
    automatically without client input.
    """

    @property
    def subscribed_topics(self) -> Set[str]:
        all_session_topics = self.parent.subscribed_topics_by_session[__not_in_a_session__]
        return super().subscribed_topics | all_session_topics
