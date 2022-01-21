from typing import Any
from typing import Callable
from typing import Generic
from typing import Optional
from typing import TypeVar

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Iterable

from saturn_engine.utils.log import getLogger

T = TypeVar("T")

AsyncFNone = TypeVar("AsyncFNone", bound=Callable[..., Awaitable[None]])


async def aiter2agen(iterator: AsyncIterator[T]) -> AsyncGenerator[T, None]:
    """
    Convert an async iterator into an async generator.
    """
    async for x in iterator:
        yield x


class TasksGroup:
    def __init__(self, tasks: Iterable[asyncio.Task] = ()) -> None:
        self.logger = getLogger(__name__, self)
        self.tasks = set(tasks)
        self.updated = asyncio.Event()
        self.updated_task = asyncio.create_task(self.updated.wait())

    def add(self, task: asyncio.Task) -> None:
        self.tasks.add(task)
        self.updated.set()

    def remove(self, task: asyncio.Task) -> None:
        self.tasks.discard(task)
        self.updated.set()

    async def wait(self) -> set[asyncio.Task]:
        self.updated.clear()
        done, _ = await asyncio.wait(
            self.tasks | {self.updated_task}, return_when=asyncio.FIRST_COMPLETED
        )

        if self.updated_task in done:
            done.remove(self.updated_task)
            self.updated_task = asyncio.create_task(self.updated.wait())

        return done

    async def close(self) -> None:
        self.updated_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.updated_task

        for task in self.tasks:
            task.cancel()
        tasks_results = await asyncio.gather(*self.tasks, return_exceptions=True)
        for result in tasks_results:
            if isinstance(result, Exception):
                self.logger.error(
                    "Task '%s' cancelled with error", task, exc_info=result
                )
        self.tasks.clear()

    def all(self) -> set[asyncio.Task]:
        return self.tasks


class DelayedThrottle(Generic[AsyncFNone]):
    __call__: AsyncFNone

    def __init__(self, func: AsyncFNone, *, delay: float) -> None:
        self.func = func
        self.delay = delay
        self.delayed_task: Optional[asyncio.Task] = None
        self.delayed_lock = asyncio.Lock()
        self.delayed_args: tuple[Any, ...] = ()
        self.delayed_kwargs: dict[str, Any] = {}

    async def __call__(self, *args: Any, **kwargs: Any) -> None:  # type: ignore
        async with self.delayed_lock:
            self.delayed_args = args
            self.delayed_kwargs = kwargs

            if self.delayed_task is None:
                self.delayed_task = asyncio.create_task(self._delay_call())

    async def _delay_call(self) -> None:
        await asyncio.sleep(self.delay)
        async with self.delayed_lock:
            await self.func(*self.delayed_args, **self.delayed_kwargs)
            self.delayed_task = None

    async def cancel(self) -> None:
        if self.delayed_task is None:
            return
        self.delayed_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.delayed_task

    async def flush(self) -> None:
        if self.delayed_task is None:
            return
        async with self.delayed_lock:
            await self.cancel()
            await self.func(*self.delayed_args, **self.delayed_kwargs)
            self.delayed_task = None
