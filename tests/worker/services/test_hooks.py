from typing import Protocol
from unittest import mock

import pytest

from saturn_engine.worker.services.hooks import AsyncEventHook
from saturn_engine.worker.services.hooks import EventHook


class EventHandler(Protocol):
    def __call__(self, a: str, *, b: str) -> None:
        ...


class AsyncEventHandler(Protocol):
    async def __call__(self, a: str, *, b: str) -> None:
        ...


def test_event_hook() -> None:
    error_handler = mock.Mock()
    hook: EventHook[EventHandler] = EventHook(error_handler=error_handler)

    handler1 = mock.Mock()
    hook.register(handler1)

    handler2 = mock.Mock()
    hook.register(handler2)

    hook.emit("a", b="c")
    handler1.assert_called_once_with("a", b="c")
    handler2.assert_called_once_with("a", b="c")

    handler1.reset_mock()
    handler2.reset_mock()

    error = ValueError()
    handler1.side_effect = error
    hook.emit("d", b="e")
    error_handler.assert_called_once_with(error)
    handler2.assert_called_once_with("d", b="e")


@pytest.mark.asyncio
async def test_async_event_hook() -> None:
    error_handler = mock.Mock()
    hook: AsyncEventHook[AsyncEventHandler] = AsyncEventHook(
        error_handler=error_handler
    )

    handler1 = mock.AsyncMock()
    hook.register(handler1)

    handler2 = mock.AsyncMock()
    hook.register(handler2)

    await hook.emit("a", b="c")
    handler1.assert_awaited_once_with("a", b="c")
    handler2.assert_awaited_once_with("a", b="c")

    handler1.reset_mock()
    handler2.reset_mock()

    error = ValueError()
    handler1.side_effect = error
    await hook.emit("d", b="e")
    error_handler.assert_called_once_with(error)
    handler2.assert_awaited_once_with("d", b="e")
