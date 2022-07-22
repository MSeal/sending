"""Utilities for common tasks"""
import inspect
from typing import Awaitable, Callable


def ensure_async(fn: Callable) -> Callable[..., Awaitable]:
    """A decorator that can be used to require async behavior."""
    if inspect.iscoroutinefunction(fn):
        return fn

    async def wrapped(*args, **kwargs):
        return fn(*args, **kwargs)

    return wrapped
