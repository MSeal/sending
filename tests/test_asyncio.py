import asyncio

import pytest


async def sleep10():
    await asyncio.sleep(10)


async def test_timeout():
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(sleep10(), timeout=1)


async def test_timeout_from_task():
    task = asyncio.create_task(sleep10())
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task, timeout=1)


async def test_timeout_from_future():
    f = asyncio.Future()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(f, timeout=1)
