from functools import partial

import pytest
from sending.backends.memory import InMemoryPubSubManager


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


async def wait_on_queues(manager: InMemoryPubSubManager):
    await manager.outbound_queue.join()
    await manager.message_queue.join()
    await manager.inbound_queue.join()


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
        await wait_on_queues(manager)
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
        await wait_on_queues(manager)
        assert len(cache) == 0
