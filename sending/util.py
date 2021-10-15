"""Utilities for common tasks"""
import inspect
from itertools import islice
from typing import Awaitable, Callable, Iterator, List


def split_collection(c, slices) -> List[Iterator]:
    """Splits collection into a number of slices, as equally-sized as possible."""
    return [islice(c, n, None, slices) for n in range(slices)]


def ensure_async(fn: Callable) -> Callable[..., Awaitable]:
    """A decorator that can be used to require async behavior."""
    if inspect.iscoroutinefunction(fn):
        return fn

    async def wrapped(*args, **kwargs):
        return fn(*args, **kwargs)

    return wrapped
