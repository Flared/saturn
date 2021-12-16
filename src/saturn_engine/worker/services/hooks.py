from collections.abc import Generator
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Generic
from typing import Optional
from typing import TypeVar
from typing import Union

TCallable = TypeVar("TCallable", bound=Callable[..., Any])
TAwaitable = TypeVar("TAwaitable", bound=Callable[..., Awaitable])

A = TypeVar("A")
R = TypeVar("R")
GGenerators = list[Generator[None, A, None]]


class EventHook(Generic[TCallable]):
    def __init__(self, *, error_handler: Optional[Callable[[Exception], None]] = None):
        self.error_handler = error_handler
        self.handlers: list[TCallable] = []

    def register(self, handler: TCallable) -> TCallable:
        self.handlers.append(handler)
        return handler

    def unregister(self, handler: TCallable) -> None:
        self.handlers.remove(handler)

    emit: TCallable

    def emit(self, *args: Any, **kwargs: Any) -> None:  # type: ignore
        for handler in self.handlers:
            try:
                handler(*args, **kwargs)
            except Exception as e:
                if self.error_handler:
                    self.error_handler(e)


class AsyncEventHook(Generic[TAwaitable]):
    def __init__(self, *, error_handler: Callable[[Exception], None]):
        self.error_handler = error_handler
        self.handlers: list[TAwaitable] = []

    def register(self, handler: TAwaitable) -> TAwaitable:
        self.handlers.append(handler)
        return handler

    def unregister(self, handler: TAwaitable) -> None:
        self.handlers.remove(handler)

    emit: TAwaitable

    async def emit(self, *args: Any, **kwargs: Any) -> None:  # type: ignore
        for handler in self.handlers:
            try:
                await handler(*args, **kwargs)
            except Exception as e:
                if self.error_handler:
                    self.error_handler(e)


class ContextHook(Generic[A, R]):
    def __init__(self, *, error_handler: Optional[Callable[[Exception], None]] = None):
        self.error_handler = error_handler
        self.handlers: list[
            Union[Callable[[A], None], Callable[[A], Generator[None, R, None]]]
        ] = []

    def register(
        self,
        handler: Union[Callable[[A], None], Callable[[A], Generator[None, R, None]]],
    ) -> Union[Callable[[A], None], Callable[[A], Generator[None, R, None]]]:
        self.handlers.append(handler)
        return handler

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
