import asyncio

from managed_service_fixtures import RedisDetails

from sending.backends.redis import RedisPubSubManager


class TestRedisBackend:
    """Redis backend tests

    Expects that a local Redis instance is being run.
    """

    async def test_redis_backend(self, mocker, managed_redis: RedisDetails):
        cb = mocker.MagicMock()
        mgr = RedisPubSubManager(dsn=managed_redis.url)
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
