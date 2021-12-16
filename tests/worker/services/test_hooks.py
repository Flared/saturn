from collections.abc import Generator
from typing import Protocol
from unittest import mock

import pytest

from saturn_engine.worker.services.hooks import AsyncEventHook
from saturn_engine.worker.services.hooks import ContextHook
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


def test_context_hook() -> None:
    error_handler = mock.Mock()
    m = mock.Mock()
    hook: ContextHook[str, str] = ContextHook(error_handler=error_handler)

    @hook.register
    def handler(a: str) -> Generator[None, str, None]:
        m(handler, "before")
        try:
            r = yield
            m(handler, "after", r)
        except Exception as e:
            m(handler, "error", e)

    @hook.register
    def handler_before(a: str) -> None:
        m(handler_before, "before")

    @hook.register
    def handler_after(a: str) -> Generator[None, str, None]:
        m(handler_after, "before")
        yield
        m(handler_after, "after")

    @hook.emit
    def scope(a: str) -> str:
        m("in", a)
        return "c"

    assert scope("1") == "c"

    m.assert_has_calls(
        [
            mock.call(handler, "before"),
            mock.call(handler_before, "before"),
            mock.call(handler_after, "before"),
            mock.call("in", "1"),
            mock.call(handler_after, "after"),
            mock.call(handler, "after", "c"),
        ]
    )

    m.reset_mock()
    e = ValueError()

    with pytest.raises(ValueError):

        @hook.emit
        def scope_raising(a: str) -> str:
            m("in", a)
            raise e

        scope_raising("1")

    m.assert_has_calls(
        [
            mock.call(handler, "before"),
            mock.call(handler_before, "before"),
            mock.call(handler_after, "before"),
            mock.call("in", "1"),
            mock.call(handler, "error", e),
        ]
    )

    error_handler.assert_not_called()


def test_context_hook_errors() -> None:
    error_handler = mock.Mock()
    m = mock.Mock()
    hook: ContextHook[None, None] = ContextHook(error_handler=error_handler)

    @hook.register
    def call_raises(_: None) -> Generator[None, None, None]:
        m(call_raises)
        raise ValueError

    @hook.register
    def generator_raises(_: None) -> Generator[None, None, None]:
        m(generator_raises)
        raise ValueError
        yield

    @hook.register
    def generator_raises_after(_: None) -> Generator[None, None, None]:
        yield
        m(generator_raises_after)
        raise ValueError

    @hook.register
    def generator_many_yield(_: None) -> Generator[None, None, None]:
        yield
        m(generator_many_yield)
        yield
        m(None)
        raise ValueError

    @hook.register
    def generator_many_yield_wont_stop(_: None) -> Generator[None, None, None]:
        yield
        try:
            yield
            m(None)
        except GeneratorExit:
            m(generator_many_yield_wont_stop)
            yield
            m(None)

    @hook.emit
    def scope(_: None) -> None:
        pass

    scope(None)

    m.assert_has_calls(
        [
            mock.call(call_raises),
            mock.call(generator_raises),
            mock.call(generator_many_yield_wont_stop),
            mock.call(generator_many_yield),
            mock.call(generator_raises_after),
        ]
    )

    assert error_handler.call_count == 5


def test_context_hook_error_errors() -> None:  # noqa: C901  # Ignore complexity errors.
    error_handler = mock.Mock()
    m = mock.Mock()
    hook: ContextHook[None, None] = ContextHook(error_handler=error_handler)

    @hook.register
    def generator_raise_in_except(_: None) -> Generator[None, None, None]:
        try:
            yield
        except Exception:
            m(generator_raise_in_except)
            raise ValueError from None

    @hook.register
    def generator_yield_in_except(_: None) -> Generator[None, None, None]:
        try:
            yield
        except Exception:
            m(generator_yield_in_except)
            yield
            m(None)

    @hook.register
    def generator_wont_stop(_: None) -> Generator[None, None, None]:
        try:
            yield
        except Exception:
            try:
                yield
            except GeneratorExit:
                m(generator_wont_stop)
                yield
                m(None)

    @hook.register
    def generator_reraise(_: None) -> Generator[None, None, None]:
        try:
            yield
        except Exception:
            m(generator_reraise)
            raise

    e = ValueError()

    with pytest.raises(ValueError):

        @hook.emit
        def scope(_: None) -> None:
            raise e

        scope(None)
    m.assert_has_calls(
        [
            mock.call(generator_reraise),
            mock.call(generator_wont_stop),
            mock.call(generator_yield_in_except),
            mock.call(generator_raise_in_except),
        ]
    )

    assert error_handler.call_count == 3
