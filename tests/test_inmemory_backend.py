from functools import partial

import pytest
from sending.backends.memory import InMemoryPubSubManager
from sending.base import __all_sessions__


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
        unsub = manager.callback(cb)
        await manager._delegate_to_callbacks("test cb", manager.callbacks_by_id.keys())
        unsub()
        assert len(cache) == 1
        assert cache[-1] == "test cb"

    async def test_register_callbacks_async(self, manager: InMemoryPubSubManager):
        cache = []
        cb = partial(async_callback, cache)
        unsub = manager.callback(cb)
        await manager._delegate_to_callbacks("test async_cb", manager.callbacks_by_id.keys())
        unsub()
        assert len(cache) == 1
        assert cache[-1] == "test async_cb"

    async def test_send_to_subscribed_topic(self, manager: InMemoryPubSubManager):
        cache = []
        cb = partial(async_callback, cache)
        await manager.subscribe_to_topic("topic")
        unsub = manager.callback(cb)
        manager.send("topic", "hello")
        await manager._drain_queues()
        unsub()
        assert len(cache) == 1
        await manager.unsubscribe_from_topic("topic")
        assert len(manager.subscribed_topics) == 0

    async def test_send_to_unsubscribed_topic(self, manager: InMemoryPubSubManager):
        cache = []
        cb = partial(callback, cache)
        unsub = manager.callback(cb)
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
