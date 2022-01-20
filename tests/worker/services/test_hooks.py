from collections.abc import AsyncGenerator
from collections.abc import Generator
from unittest import mock

import pytest

from saturn_engine.utils.hooks import AsyncContextHook
from saturn_engine.utils.hooks import AsyncEventHook
from saturn_engine.utils.hooks import ContextHook
from saturn_engine.utils.hooks import EventHook


def test_event_hook() -> None:
    error_handler = mock.Mock()
    hook: EventHook[str] = EventHook(error_handler=error_handler)

    handler1 = mock.Mock()
    hook.register(handler1)

    handler2 = mock.Mock()
    hook.register(handler2)

    hook.emit("a")
    handler1.assert_called_once_with("a")
    handler2.assert_called_once_with("a")

    handler1.reset_mock()
    handler2.reset_mock()

    error = ValueError()
    handler1.side_effect = error
    hook.emit("d")
    error_handler.assert_called_once_with(error)
    handler2.assert_called_once_with("d")


@pytest.mark.asyncio
async def test_async_event_hook() -> None:
    error_handler = mock.AsyncMock()
    hook: AsyncEventHook[str] = AsyncEventHook(error_handler=error_handler)

    handler1 = mock.AsyncMock()
    hook.register(handler1)

    handler2 = mock.AsyncMock()
    hook.register(handler2)

    await hook.emit("a")
    handler1.assert_awaited_once_with("a")
    handler2.assert_awaited_once_with("a")

    handler1.reset_mock()
    handler2.reset_mock()

    error = ValueError()
    handler1.side_effect = error
    await hook.emit("d")
    error_handler.assert_awaited_once_with(error)
    handler2.assert_awaited_once_with("d")


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


@pytest.mark.asyncio
async def test_async_context_hook() -> None:
    error_handler = mock.AsyncMock()
    m = mock.Mock()
    hook: AsyncContextHook[str, str] = AsyncContextHook(error_handler=error_handler)

    @hook.register
    async def handler(a: str) -> AsyncGenerator[None, str]:
        m(handler, "before")
        try:
            r = yield
            m(handler, "after", r)
        except Exception as e:
            m(handler, "error", e)

    @hook.register
    async def handler_before(a: str) -> None:
        m(handler_before, "before")

    @hook.register
    async def handler_after(a: str) -> AsyncGenerator[None, str]:
        m(handler_after, "before")
        yield
        m(handler_after, "after")

    @hook.emit
    async def scope(a: str) -> str:
        m("in", a)
        return "c"

    assert await scope("1") == "c"

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
        async def scope_raising(a: str) -> str:
            m("in", a)
            raise e

        await scope_raising("1")

    m.assert_has_calls(
        [
            mock.call(handler, "before"),
            mock.call(handler_before, "before"),
            mock.call(handler_after, "before"),
            mock.call("in", "1"),
            mock.call(handler, "error", e),
        ]
    )

    error_handler.assert_not_awaited()


def test_context_hook_errors() -> None:
    error_handler = mock.Mock()
    m = mock.Mock()
    hook: ContextHook[None, None] = ContextHook(error_handler=error_handler)

    @hook.register
    def no_yield(_: None) -> Generator[None, None, None]:
        m(no_yield)
        if False:
            yield

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
            mock.call(no_yield),
            mock.call(call_raises),
            mock.call(generator_raises),
            mock.call(generator_many_yield_wont_stop),
            mock.call(generator_many_yield),
            mock.call(generator_raises_after),
        ]
    )

    assert error_handler.call_count == 6


def test_context_hook_error_errors() -> None:
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


@pytest.mark.asyncio
async def test_async_context_hook_errors() -> None:
    error_handler = mock.AsyncMock()
    m = mock.Mock()
    hook: AsyncContextHook[None, None] = AsyncContextHook(error_handler=error_handler)

    @hook.register
    async def no_yield(_: None) -> AsyncGenerator[None, None]:
        m(no_yield)
        if False:
            yield

    @hook.register
    async def call_raises(_: None) -> None:
        m(call_raises)
        raise ValueError

    @hook.register
    async def generator_raises(_: None) -> AsyncGenerator[None, None]:
        m(generator_raises)
        raise ValueError
        yield

    @hook.register
    async def generator_raises_after(_: None) -> AsyncGenerator[None, None]:
        yield
        m(generator_raises_after)
        raise ValueError

    @hook.register
    async def generator_many_yield(_: None) -> AsyncGenerator[None, None]:
        yield
        m(generator_many_yield)
        yield
        m(None)
        raise ValueError

    @hook.register
    async def generator_many_yield_wont_stop(_: None) -> AsyncGenerator[None, None]:
        yield
        try:
            yield
            m(None)
        except GeneratorExit:
            m(generator_many_yield_wont_stop)
            yield
            m(None)

    @hook.emit
    async def scope(_: None) -> None:
        pass

    await scope(None)

    m.assert_has_calls(
        [
            mock.call(no_yield),
            mock.call(call_raises),
            mock.call(generator_raises),
            mock.call(generator_many_yield_wont_stop),
            mock.call(generator_many_yield),
            mock.call(generator_raises_after),
        ]
    )

    assert error_handler.await_count == 6


@pytest.mark.asyncio
async def test_async_context_hook_error_errors() -> None:
    error_handler = mock.AsyncMock()
    m = mock.Mock()
    hook: AsyncContextHook[None, None] = AsyncContextHook(error_handler=error_handler)

    @hook.register
    async def generator_raise_in_except(_: None) -> AsyncGenerator[None, None]:
        try:
            yield
        except Exception:
            m(generator_raise_in_except)
            raise ValueError from None

    @hook.register
    async def generator_yield_in_except(_: None) -> AsyncGenerator[None, None]:
        try:
            yield
        except Exception:
            m(generator_yield_in_except)
            yield
            m(None)

    @hook.register
    async def generator_wont_stop(_: None) -> AsyncGenerator[None, None]:
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
    async def generator_reraise(_: None) -> AsyncGenerator[None, None]:
        try:
            yield
        except Exception:
            m(generator_reraise)
            raise

    e = ValueError()

    with pytest.raises(ValueError):

        @hook.emit
        async def scope(_: None) -> None:
            raise e

        await scope(None)
    m.assert_has_calls(
        [
            mock.call(generator_reraise),
            mock.call(generator_wont_stop),
            mock.call(generator_yield_in_except),
            mock.call(generator_raise_in_except),
        ]
    )

    assert error_handler.await_count == 3
