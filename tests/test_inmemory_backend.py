from functools import partial

import pytest

from sending.backends.memory import InMemoryPubSubManager
from sending.base import __all_sessions__, QueuedMessage


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


@pytest.mark.asyncio
class TestInMemoryPubSubManager:
    async def test_register_callbacks(self, manager: InMemoryPubSubManager):
        cache = []
        cb = partial(callback, cache)
        await manager.subscribe_to_topic("test")
        unsub = manager.register_callback(cb)
        await manager._delegate_to_callbacks(
            QueuedMessage("test", "test cb", None), manager.callbacks_by_id.keys()
        )
        unsub()
        assert len(cache) == 1
        assert cache[-1] == "test cb"

    async def test_register_callbacks_async(self, manager: InMemoryPubSubManager):
        cache = []
        cb = partial(async_callback, cache)
        await manager.subscribe_to_topic("test")
        unsub = manager.register_callback(cb)
        await manager._delegate_to_callbacks(
            QueuedMessage("test", "test async_cb", None), manager.callbacks_by_id.keys()
        )
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
        await manager.subscribe_to_topic("topic", __all_sessions__)
        await manager.subscribe_to_topic("topic", "test-session")
        assert manager.subscribed_topics == {"topic"}
        assert manager.is_subscribed_to_topic("topic")
        await manager.unsubscribe_from_topic("topic", __all_sessions__)
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
            await manager._delegate_to_callbacks(
                QueuedMessage("test", "test cb", None), manager.callbacks_by_id.keys()
            )
            unsub()
            assert len(cache) == 1
            assert cache[-1] == "test cb"

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
        async def predicate(message):
            return message.contents == "message"

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

    async def test_sending_to_session_callbacks(self, manager: InMemoryPubSubManager):
        async with manager.get_session() as session:
            cache = []
            cb = partial(callback, cache)
            session.register_callback(cb)
            session.send_to_callbacks("message")
            await manager._drain_queues()
            assert len(cache) == 1
            assert cache[0] == "message"


@pytest.mark.asyncio
class TestPubSubSession:
    async def test_when_parent_is_subscribed_to_multiple_topics_the_session_only_receives_messages_its_subscribed_to(
        self, manager
    ):
        cache1 = []
        cb1 = partial(async_callback, cache1)
        cache2 = []
        cb2 = partial(async_callback, cache2)

        session1 = manager.get_session()
        session2 = manager.get_session()

        session1.register_callback(cb1)
        session2.register_callback(cb2)
        await session1.subscribe_to_topic("files/1")
        await session2.subscribe_to_topic("files/2")

        manager.send("files/1", "hello")
        manager.send("files/2", "foo")
        await manager._drain_queues()

        assert cache1 == ["hello"]
        assert cache2 == ["foo"]
