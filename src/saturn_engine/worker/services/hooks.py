from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Generic
from typing import TypeVar

TCallable = TypeVar("TCallable", bound=Callable[..., Any])
TAwaitable = TypeVar("TAwaitable", bound=Callable[..., Awaitable])


class EventHook(Generic[TCallable]):
    def __init__(self, *, error_handler: Callable[[Exception], None]):
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
