from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Generic
from typing import Optional
from typing import TypeVar
from typing import Union

from collections.abc import AsyncGenerator
from collections.abc import Generator

import asyncstdlib as alib

F = TypeVar("F", bound=Callable)

A = TypeVar("A")
R = TypeVar("R")
E = TypeVar("E")
GGenerators = list[Generator[None, A, None]]
AGenerators = list[AsyncGenerator[None, A]]


class Handlers(Generic[F, E]):
    def __init__(self, *, error_handler: Optional[Callable[[Exception], E]] = None):
        self.error_handler = error_handler
        self.handlers: list[F] = []

    def register(self, handler: F) -> Callable[[A], Any]:
        self.handlers.append(handler)
        return handler

    def unregister(self, handler: F) -> None:
        self.handlers.remove(handler)

    def __bool__(self) -> bool:
        return bool(self.handlers)


class EventHook(Generic[A], Handlers[Callable[[A], Any], None]):
    def emit(self, arg: A) -> None:
        for handler in self.handlers:
            try:
                handler(arg)
            except Exception as e:
                if self.error_handler:
                    self.error_handler(e)


class AsyncEventHook(Generic[A], Handlers[Callable[[A], Awaitable], Awaitable]):
    async def emit(self, arg: A) -> None:
        for handler in self.handlers:
            try:
                await handler(arg)
            except Exception as e:
                if self.error_handler:
                    await self.error_handler(e)


class ContextHook(
    Generic[A, R],
    Handlers[Union[Callable[[A], Any], Callable[[A], Generator[None, R, None]]], None],
):
    def emit(self, scope: Callable[[A], R]) -> "ContextHookEmiter[A, R]":
        return ContextHookEmiter(self, scope)


class ContextHookEmiter(Generic[A, R]):
    def __init__(self, hook: ContextHook[A, R], scope: Callable[[A], R]):
        self.hook = hook
        self.scope = scope

    def __call__(self, arg: A) -> R:
        generators = self.on_call(arg)
        try:
            result = self.scope(arg)
            self.on_result(generators, result)
        except Exception as e:
            self.on_error(generators, e)
            raise
        return result

    def on_call(self, arg: A) -> GGenerators:
        # Call handlers and collect generators
        generators = []
        for handler in self.hook.handlers:
            try:
                result = handler(arg)
                if isinstance(result, Generator):
                    next(result)
                    generators.append(result)
            except Exception as e:
                self.handle_error(e)
        return generators

    def on_error(self, generators: GGenerators, error: Exception) -> None:
        # If `scope` raise, propagate the error to all handlers.
        for generator in reversed(generators):
            try:
                generator.throw(error)
                # Ensure the generator is closed.
                generator.close()
                try:
                    raise ValueError("Handler must yield once")
                except Exception as e:
                    self.handle_error(e)
            except StopIteration:
                # The generator could be done.
                pass
            except Exception as gen_e:
                # Ensure the error raised is not the one that was passed.
                if gen_e is not error:
                    self.handle_error(gen_e)

    def on_result(self, generators: GGenerators, result: R) -> None:
        # Call handlers in reverse order with the result
        for generator in reversed(generators):
            try:
                generator.send(result)
                # Ensure the generator is closed.
                generator.close()
                try:
                    raise ValueError("Handler must yield once")
                except Exception as e:
                    self.handle_error(e)
            except StopIteration:
                # This is expected.
                pass
            except Exception as e:
                self.handle_error(e)

    def handle_error(self, error: Exception) -> None:
        if self.hook.error_handler:
            self.hook.error_handler(error)


class AsyncContextHook(
    Generic[A, R],
    Handlers[
        Union[Callable[[A], Awaitable], Callable[[A], AsyncGenerator[None, R]]],
        Awaitable,
    ],
):
    def emit(
        self, scope: Callable[[A], Awaitable[R]]
    ) -> "AsyncContextHookEmiter[A, R]":
        return AsyncContextHookEmiter(self, scope)


class AsyncContextHookEmiter(Generic[A, R]):
    def __init__(
        self, hook: AsyncContextHook[A, R], scope: Callable[[A], Awaitable[R]]
    ):
        self.hook = hook
        self.scope = scope

    async def __call__(self, arg: A) -> R:
        generators = await self.on_call(arg)
        try:
            result = await self.scope(arg)
            await self.on_result(generators, result)
        except Exception as e:
            await self.on_error(generators, e)
            raise
        return result

    async def on_call(self, arg: A) -> AGenerators:
        # Call handlers and collect generators
        generators = []
        for handler in self.hook.handlers:
            try:
                result = handler(arg)
                if isinstance(result, AsyncGenerator):
                    await alib.anext(result)
                    generators.append(result)
                else:
                    await result
            except Exception as e:
                await self.handle_error(e)
        return generators

    async def on_error(self, generators: AGenerators, error: Exception) -> None:
        # If `scope` raise, propagate the error to all handlers.
        for generator in reversed(generators):
            try:
                await generator.athrow(error)
                # Ensure the generator is closed.
                await generator.aclose()
                try:
                    raise ValueError("Handler must yield once")
                except Exception as e:
                    await self.handle_error(e)
            except StopAsyncIteration:
                # The generator could be done.
                pass
            except Exception as gen_e:
                # Ensure the error raised is not the one that was passed.
                if gen_e is not error:
                    await self.handle_error(gen_e)

    async def on_result(self, generators: AGenerators, result: R) -> None:
        # Call handlers in reverse order with the result
        for generator in reversed(generators):
            try:
                await generator.asend(result)
                # Ensure the generator is closed.
                await generator.aclose()
                try:
                    raise ValueError("Handler must yield once")
                except Exception as e:
                    await self.handle_error(e)
            except StopAsyncIteration:
                # This is expected.
                pass
            except Exception as e:
                await self.handle_error(e)

    async def handle_error(self, error: Exception) -> None:
        if self.hook.error_handler:
            await self.hook.error_handler(error)
