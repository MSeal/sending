import asyncio
import os

import pytest

from sending.backends.redis import RedisPubSubManager

REDIS_DSN = os.getenv("REDIS_DSN")


@pytest.mark.redis
class TestRedisBackend:
    """Redis backend tests

    Expects that a local Redis instance is being run.
    """

    @pytest.mark.asyncio
    async def test_redis_backend(self, mocker):
        cb = mocker.MagicMock()
        mgr = RedisPubSubManager(REDIS_DSN)
        await mgr.initialize()
        mgr.register_callback(cb)

        async with mgr._redis.client() as conn:
            await conn.ping()

        await mgr.subscribe_to_topic("topic")
        mgr.send("topic", "test!")
        await asyncio.sleep(1)
        await mgr._drain_queues()
        cb.assert_called_once()

        await mgr.unsubscribe_from_topic("topic")
        mgr.send("topic", "test!")
        await asyncio.sleep(1)
        await mgr.shutdown()
        cb.assert_called_once()
