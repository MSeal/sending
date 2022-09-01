import asyncio
from functools import partial

import pytest

from sending.logging import logger
from sending.backends.memory import InMemoryPubSubManager
from sending.base import QueuedMessage, SystemEvents, __not_in_a_session__


@pytest.fixture()
async def manager():
    mgr = InMemoryPubSubManager()
    await mgr.initialize()
    yield mgr
    await mgr.shutdown(now=True)


def callback(iterable, message):
    iterable.append(message)


async def async_callback(iterable, message):
    iterable.append(message)


class TestInMemoryPubSubManager:
    async def test_register_callbacks(self, manager: InMemoryPubSubManager):
        cache = []
        cb = partial(callback, cache)
        await manager.subscribe_to_topic("test")
        unsub = manager.register_callback(cb)
        for id, cb in manager.callbacks_by_id.items():
            await manager._delegate_to_callback(QueuedMessage("test", "test cb", None), id)
        unsub()
        assert len(cache) == 1
        assert cache[-1] == "test cb"

    async def test_register_callbacks_async(self, manager: InMemoryPubSubManager):
        cache = []
        cb = partial(async_callback, cache)
        await manager.subscribe_to_topic("test")
        unsub = manager.register_callback(cb)
        for id, cb in manager.callbacks_by_id.items():
            await manager._delegate_to_callback(QueuedMessage("test", "test async_cb", None), id)
        unsub()
        assert len(cache) == 1
        assert cache[-1] == "test async_cb"

    async def test_send_to_subscribed_topic(self, manager: InMemoryPubSubManager):
        cache = []
        cb = partial(async_callback, cache)
        await manager.subscribe_to_topic("topic")
        unsub = manager.register_callback(cb)
        manager.send("topic", "hello")
        await manager._drain_queues()
        unsub()
        assert len(cache) == 1
        await manager.unsubscribe_from_topic("topic")
        assert len(manager.subscribed_topics) == 0

    async def test_send_to_unsubscribed_topic(self, manager: InMemoryPubSubManager):
        cache = []
        cb = partial(callback, cache)
        unsub = manager.register_callback(cb)
        manager.send("topic", "hello")
        unsub()
        await manager._drain_queues()
        assert len(cache) == 0

    async def test_unsub_from_unsubscribed_topic(self, manager: InMemoryPubSubManager):
        await manager.unsubscribe_from_topic("topic")
        assert len(manager.subscribed_topics) == 0

    async def test_subscriptions_across_multiple_sessions(self, manager: InMemoryPubSubManager):
        await manager.subscribe_to_topic("topic", __not_in_a_session__)
        await manager.subscribe_to_topic("topic", "test-session")
        assert manager.subscribed_topics == {"topic"}
        assert manager.is_subscribed_to_topic("topic")
        await manager.unsubscribe_from_topic("topic", __not_in_a_session__)
        assert manager.is_subscribed_to_topic("topic")
        await manager.unsubscribe_from_topic("topic", "test-session")
        assert len(manager.subscribed_topics) == 0
        assert not manager.is_subscribed_to_topic("topic")

    async def test_session_subscriptions(self, manager: InMemoryPubSubManager):
        async with manager.get_session() as session:
            await session.subscribe_to_topic("topic")
            assert session.is_subscribed_to_topic("topic")
            assert manager.is_subscribed_to_topic("topic")
            await session.unsubscribe_from_topic("topic")
            assert not session.is_subscribed_to_topic("topic")
            assert not manager.is_subscribed_to_topic("topic")

    async def test_session_callbacks(self, manager: InMemoryPubSubManager):
        async with manager.get_session() as session:
            cache = []
            cb = partial(callback, cache)
            await session.subscribe_to_topic("test")
            unsub = session.register_callback(cb)
            for id, cb in manager.callbacks_by_id.items():
                await manager._delegate_to_callback(QueuedMessage("test", "test cb", None), id)
            unsub()
            assert len(cache) == 1
            assert cache[-1] == "test cb"

    async def test_session_inherit_global_subscription(self, manager: InMemoryPubSubManager):
        await manager.subscribe_to_topic("test")
        async with manager.get_session() as session:
            cache = []
            cb = partial(callback, cache)
            unsub = session.register_callback(cb)
            for id, cb in manager.callbacks_by_id.items():
                await manager._delegate_to_callback(QueuedMessage("test", "test cb", None), id)
            unsub()
            assert len(cache) == 1
            assert cache[-1] == "test cb"

    async def test_detached_session_does_not_inherit_global_subscription(
        self, manager: InMemoryPubSubManager
    ):
        await manager.subscribe_to_topic("test")
        async with manager.get_detached_session() as session:
            cache = []
            cb = partial(callback, cache)
            unsub = session.register_callback(cb)
            for id, cb in manager.callbacks_by_id.items():
                await manager._delegate_to_callback(QueuedMessage("test", "test cb", None), id)
            unsub()
            assert len(cache) == 0

    async def test_session_async_exit(self, manager: InMemoryPubSubManager):
        async with manager.get_session() as session:
            cb = partial(callback, [])
            session.register_callback(cb)
            await session.subscribe_to_topic("topic")

        assert not session.is_subscribed_to_topic("topic")
        assert not manager.is_subscribed_to_topic("topic")
        assert len(session._unregister_callbacks_by_id) == 0
        assert len(manager.callbacks_by_id) == 0

    async def test_manager_shutdown(self, manager: InMemoryPubSubManager):
        cb = partial(async_callback, [])
        await manager.subscribe_to_topic("topic")
        manager.register_callback(cb)
        await manager.shutdown(now=False)
        assert manager.inbound_queue is None
        assert manager.outbound_queue is None
        assert len(manager.subscribed_topics) == 0
        assert len(manager.callbacks_by_id) == 0
        assert len(manager.poll_workers) == 0
        assert len(manager.inbound_workers) == 0
        assert len(manager.outbound_workers) == 0

    async def test_manager_dedupes_subscriptions(self, manager: InMemoryPubSubManager):
        await manager.subscribe_to_topic("topic")
        await manager.subscribe_to_topic("topic")
        assert len(manager.subscribed_topics) == 1
        await manager.unsubscribe_from_topic("topic")
        assert len(manager.subscribed_topics) == 0

    async def test_inbound_hook(self, manager: InMemoryPubSubManager):
        def hook(contents):
            return "hooked message!"

        cache = []
        cb = partial(callback, cache)
        manager.register_callback(cb)
        manager.inbound_message_hook = hook
        await manager.subscribe_to_topic("topic")
        manager.send("topic", "message")
        await manager._drain_queues()
        assert len(cache) == 1
        assert cache[0] == "hooked message!"

    async def test_outbound_hook(self, manager: InMemoryPubSubManager):
        def hook(contents):
            return "hooked message!"

        cache = []
        cb = partial(callback, cache)
        manager.register_callback(cb)
        manager.outbound_message_hook = hook
        await manager.subscribe_to_topic("topic")
        manager.send("topic", "message")
        await manager._drain_queues()
        assert len(cache) == 1
        assert cache[0] == "hooked message!"

    async def test_predicated_callback(self, manager: InMemoryPubSubManager):
        async def predicate(topic, contents):
            return contents == "message"

        cache = []
        cb = partial(callback, cache)
        manager.register_callback(cb, on_predicate=predicate)

        await manager.subscribe_to_topic("other_topic")
        manager.send("other_topic", "other message")
        await manager._drain_queues()
        assert len(cache) == 0

        await manager.subscribe_to_topic("topic")
        manager.send("topic", "message")
        await manager._drain_queues()
        assert len(cache) == 1
        assert cache[0] == "message"

    async def test_callback_on_topic(self, manager: InMemoryPubSubManager):
        cache = []
        cb = partial(callback, cache)
        manager.register_callback(cb, on_topic="topic")

        await manager.subscribe_to_topic("other_topic")
        manager.send("other_topic", "other message")
        await manager._drain_queues()
        assert len(cache) == 0

        await manager.subscribe_to_topic("topic")
        manager.send("topic", "message")
        await manager._drain_queues()
        assert len(cache) == 1
        assert cache[0] == "message"

    async def test_callback_decorator(self, manager: InMemoryPubSubManager):
        cache = []

        @manager.callback()
        def callback(contents):
            cache.append(contents)

        for id, cb in manager.callbacks_by_id.items():
            await manager._delegate_to_callback(QueuedMessage("test", "test cb", None), id)
        assert len(cache) == 1
        assert cache[-1] == "test cb"

    async def test_callback_decorator_topic(self, manager: InMemoryPubSubManager):
        cache = []

        @manager.callback(on_topic="topic")
        def callback(contents):
            cache.append(contents)

        for id, cb in manager.callbacks_by_id.items():
            await manager._delegate_to_callback(QueuedMessage("test", "test cb", None), id)
        assert len(cache) == 0

        for id, cb in manager.callbacks_by_id.items():
            await manager._delegate_to_callback(QueuedMessage("topic", "test cb", None), id)
        assert len(cache) == 1

    async def test_callback_decorator_predicate(self, manager: InMemoryPubSubManager):
        cache = []

        def predicate(topic, message):
            return message == "one"

        @manager.callback(on_predicate=predicate)
        def callback(contents):
            cache.append(contents)

        for id, cb in manager.callbacks_by_id.items():
            await manager._delegate_to_callback(QueuedMessage("test", "one", None), id)
            await manager._delegate_to_callback(QueuedMessage("test", "two", None), id)
        assert len(cache) == 1

    async def test_sending_to_session_callbacks(self, manager: InMemoryPubSubManager):
        async with manager.get_session() as session:
            cache = []
            cb = partial(callback, cache)
            session.register_callback(cb)
            session.send_to_callbacks("message")
            await manager._drain_queues()
            assert len(cache) == 1
            assert cache[0] == "message"

    async def test_system_events(self, manager: InMemoryPubSubManager, mocker):
        def dummy(*args):
            return True

        topic_cb = mocker.MagicMock()
        system_event_cb = mocker.MagicMock()
        manager.register_callback(topic_cb, on_topic="topic")
        manager.register_callback(topic_cb, on_predicate=dummy)
        manager.register_callback(system_event_cb, on_system_event=SystemEvents.FORCED_DISCONNECT)
        manager._emit_system_event("topic", SystemEvents.FORCED_DISCONNECT)
        await manager._drain_queues()
        system_event_cb.assert_called()
        topic_cb.assert_not_called()

    async def test_initialize_disabled_polling(self, mocker):
        """
        test that using initialize(enable_polling=False) still starts the inbound and
        outbound worker queues, so that registering callbacks that use .send() and
        triggering those with .schedule_for_delivery run and eventually delegate messages
        to ._publish.
        """
        mgr = InMemoryPubSubManager()
        publish = mocker.patch.object(mgr, "_publish")

        @mgr.callback(on_topic="")
        def echo(msg: str):
            mgr.send(topic_name="", message=msg)

        # TODO This next line needs checking since it triggers the warnings
        # TODO not sure if the mock needs adjusting or the initialize logic in abstract and backend class
        await mgr.initialize(enable_polling=True)

        mgr.schedule_for_delivery(topic="", contents="echo test")
        await asyncio.sleep(0.01)
        publish.assert_called_once_with(
            QueuedMessage(topic="", contents="echo test", session_id=None)
        )
