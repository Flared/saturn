import typing as t

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Coroutine
from collections.abc import Iterable

from saturn_engine.utils.log import getLogger

R = t.TypeVar("R")
T = t.TypeVar("T")
K = t.TypeVar("K")
V = t.TypeVar("V")

AsyncFNone = t.TypeVar("AsyncFNone", bound=t.Callable[..., Awaitable])


@contextlib.asynccontextmanager
async def opened_acontext(ctx: t.AsyncContextManager, value: T) -> t.AsyncIterator[T]:
    async with contextlib.AsyncExitStack() as stack:
        stack.push_async_exit(ctx)
        yield value


async def aiter2agen(iterator: AsyncIterator[T]) -> AsyncGenerator[T, None]:
    """
    Convert an async iterator into an async generator.
    """
    async for x in iterator:
        yield x


class TasksGroup:
    def __init__(
        self, tasks: Iterable[asyncio.Task] = (), *, name: t.Optional[str] = None
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.tasks = set(tasks)
        self.updated = asyncio.Event()
        self.name = name

        name = f"task-group-{self.name}.wait" if self.name else None
        self.updated_task = asyncio.create_task(self.updated.wait(), name=name)

    def add(self, task: asyncio.Task) -> None:
        self.tasks.add(task)
        self.notify()

    def create_task(self, coroutine: Coroutine, **kwargs: t.Any) -> asyncio.Task:
        task = asyncio.create_task(coroutine, **kwargs)
        self.add(task)
        return task

    def remove(self, task: asyncio.Task) -> None:
        self.tasks.discard(task)
        self.notify()

    def notify(self) -> None:
        self.updated.set()

    async def wait(self, *, remove: bool = True) -> set[asyncio.Task]:
        done, _ = await asyncio.wait(
            self.tasks | {self.updated_task}, return_when=asyncio.FIRST_COMPLETED
        )

        if self.updated_task in done:
            self.updated.clear()
            done.remove(self.updated_task)
            name = f"task-group-{self.name}.wait" if self.name else None
            self.updated_task = asyncio.create_task(self.updated.wait(), name=name)

        if remove:
            self.tasks.difference_update(done)
        return done

    async def wait_all(self) -> set[asyncio.Task]:
        if not self.tasks:
            return set()
        done, _ = await asyncio.wait(self.tasks)
        self.tasks.difference_update(done)
        return done

    async def close(self, timeout: t.Optional[float] = None) -> None:
        # Cancel the update event task.
        self.updated_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.updated_task

        if not self.tasks:
            return

        # Give the chance of all task to terminate within 'timeout'.
        if timeout:
            done, _ = await asyncio.wait(self.tasks, timeout=timeout)

        # Cancel all remaining tasks.
        for task in self.tasks:
            if not task.done():
                task.cancel()

        if not self.tasks:
            return

        # Collect results to log errors.
        done, pending = await asyncio.wait(self.tasks, timeout=timeout)
        for task in done:
            if not task.cancelled() and isinstance(task.exception(), Exception):
                self.logger.error(
                    "Task '%s' cancelled with error", task, exc_info=task.exception()
                )
        for task in pending:
            self.logger.error("Task '%s' won't complete", task)

        self.tasks.clear()

    async def __aenter__(self) -> "TasksGroup":
        return self

    async def __aexit__(self, *exc_info: t.Any) -> None:
        await self.close()

    def all(self) -> set[asyncio.Task]:
        return self.tasks


class TasksGroupRunner(TasksGroup):
    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        self._runner_task: t.Optional[asyncio.Task]
        self.is_running = False

    def start(self) -> asyncio.Task:
        if self.is_running:
            raise RuntimeError("task group is already running")
        self.is_running = True
        name = f"task-group-{self.name}.run" if self.name else None
        self._runner_task = asyncio.create_task(self.run(), name=name)
        return self._runner_task

    def stop(self) -> None:
        self.is_running = False
        self.notify()

    async def close(
        self, timeout: t.Optional[float] = None, *, wait_all: bool = False
    ) -> None:
        if self.is_running:
            # Stop the runner.
            self.stop()
            # Wait for the running task to complete.
            if self._runner_task:
                await self._runner_task

        if wait_all:
            tasks = await asyncio.wait_for(self.wait_all(), timeout=timeout)
            self._log_tasks(tasks)

        # Clean the tasks.
        await super().close(timeout=timeout)

    async def run(self) -> None:
        while self.is_running:
            done = await self.wait()
            self._log_tasks(done)

    def _log_tasks(self, tasks: set[asyncio.Task]) -> None:
        for task in tasks:
            if not task.cancelled() and isinstance(task.exception(), Exception):
                self.logger.error("Task '%s' failed", task, exc_info=task.exception())


class DelayedThrottle(t.Generic[AsyncFNone]):
    def __init__(self, func: AsyncFNone, *, delay: float) -> None:
        self.func = func
        self.delay = delay
        self.flush_event = asyncio.Event()
        self.delayed_task: t.Optional[asyncio.Task] = None
        self.delayed_params: t.Optional[
            tuple[tuple[t.Any, ...], dict[str, t.Any]]
        ] = None
        self.current_params: t.Optional[
            tuple[tuple[t.Any, ...], dict[str, t.Any]]
        ] = None

        self._call_fut: t.Optional[asyncio.Future] = None

    @property
    def is_idle(self) -> bool:
        return self.delayed_task is None

    @property
    def is_waiting(self) -> bool:
        return bool(self.delayed_params)

    @property
    def is_running(self) -> bool:
        return not self.is_idle and not self.is_waiting

    def call_nowait(self, *args: t.Any, **kwargs: t.Any) -> None:
        self.delayed_params = (args, kwargs)

        if self.is_idle:
            name = f"{self.func.__qualname__}.delayed"
            self.delayed_task = asyncio.create_task(self._delay_call(), name=name)

    def __call__(self, *args: t.Any, **kwargs: t.Any) -> asyncio.Future:
        if self._call_fut is None:
            self._call_fut = asyncio.Future()

        self.call_nowait(*args, **kwargs)
        return self._call_fut

    async def _delay_call(self) -> None:
        call_fut = None
        try:
            try:
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.flush_event.wait(), timeout=self.delay)
            finally:
                self.flush_event.clear()
                # Ensure we set call_fut, even if the wait_for raised an exception.
                call_fut = self._call_fut

            if not self.delayed_params:
                if call_fut:
                    call_fut.cancel()
                return

            args, kwargs = self.delayed_params
            self.delayed_params = None
            self._call_fut = None
            result = await self.func(*args, **kwargs)
            if call_fut:
                call_fut.set_result(result)
        except BaseException as e:
            if call_fut:
                call_fut.set_exception(e)
            raise
        finally:
            self.delayed_task = None

            # If __call__ was called while we were calling func, we requeue a new task.
            if self.delayed_params:
                args, kwargs = self.delayed_params
                self.call_nowait(*args, **kwargs)

    async def cancel(self) -> None:
        self.delayed_params = None
        if self.delayed_task is None:
            return

        self.delayed_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.delayed_task

    async def flush(self, timeout: t.Optional[int] = None) -> None:
        if self.delayed_task is None:
            return

        self.flush_event.set()
        await asyncio.wait_for(self.delayed_task, timeout=timeout)


def print_tasks_summary(loop: t.Optional[asyncio.AbstractEventLoop] = None) -> None:
    loop = loop or asyncio.get_running_loop()
    tasks = list(asyncio.all_tasks(loop))
    tasks.sort(key=lambda t: t.get_name())
    for task in tasks:
        print(task.get_name() + f" <{task._state}>")
        stack = task.get_stack()
        if stack:
            frame = stack[-1]
            print(f"  {frame.f_code.co_name}:{frame.f_lineno}")


class CachedProperty(t.Generic[T]):
    def __init__(self, getter: t.Callable[[t.Any], Coroutine[t.Any, t.Any, T]]) -> None:
        self.__wrapped__ = getter
        self._name = getter.__name__
        self.__doc__ = getter.__doc__

    def __set_name__(self, owner: t.Any, name: str) -> None:
        # Check whether we can store anything on the instance
        # Note that this is a failsafe, and might fail ugly.
        # People who are clever enough to avoid this heuristic
        # should also be clever enough to know the why and what.
        if not any("__dict__" in dir(cls) for cls in owner.__mro__):
            raise TypeError(
                "'cached_property' requires '__dict__' "
                f"on {owner.__name__!r} to store {name}"
            )
        self._name = name

    def __get__(self, instance: t.Any, owner: t.Any) -> t.Any:
        if instance is None:
            return self
        return self._get_attribute(instance)

    def __delete__(self, instance: t.Any) -> None:
        instance.__dict__.pop(self._name, None)

    async def _get_attribute(self, instance: t.Any) -> T:
        future = instance.__dict__.get(self._name)
        if future is None:
            future = asyncio.Future()
            instance.__dict__[self._name] = future

            async def wrapper() -> None:
                try:
                    value = await self.__wrapped__(instance)
                    future.set_result(value)
                except BaseException as e:
                    future.set_exception(e)
                    instance.__dict__.pop(self._name, None)

            asyncio.create_task(
                wrapper(),
                name=f"cached_property({instance.__class__.__name__}.{self._name})",
            )

        return await future


class AsyncLazyDict(t.Generic[K, V]):
    def __init__(
        self, initializer: t.Callable[[K], t.Coroutine[t.Any, t.Any, V]]
    ) -> None:
        self.data: dict[K, asyncio.Future[V]] = {}
        self._initializer = initializer

    def clear(self) -> None:
        self.data = {}

    def __iter__(self) -> t.Iterator[asyncio.Future[V]]:
        return iter(self.data.values())

    async def get(self, key: K) -> V:
        data = self.data
        value = data.get(key)
        if value:
            return await value

        task = asyncio.create_task(self._initializer(key))
        data[key] = task
        try:
            fut_value = await task
        except BaseException:
            del data[key]
            raise

        future: asyncio.Future[V] = asyncio.Future()
        future.set_result(fut_value)
        data[key] = future
        return fut_value


cached_property = CachedProperty


class WouldBlock(Exception):
    pass


class FakeSemaphore(contextlib.AbstractAsyncContextManager):
    def locked(self) -> bool:
        return False

    async def __aexit__(self, *exc: object) -> None:
        return None


class SharedLock:
    """Like a lock, but bind a lock to a reservation.
    * A reservation can only be made if there's no active lock.
    * A reservation can lock, blocking any new reservation or reservation
      locking.
    * Once reservation is locked, this reservation can be locked many time
      without blocking.
    """

    def __init__(self, *, max_reservations: int = 0):
        self._lock = asyncio.Lock()
        self._locker: t.Optional[object] = None
        if max_reservations:
            self._reservations_lock = asyncio.Semaphore(max_reservations)
        else:
            self._reservations_lock = t.cast(asyncio.Semaphore, FakeSemaphore())

    @contextlib.asynccontextmanager
    async def reserve(self) -> AsyncIterator["SharedLockReservation"]:
        async with self._reservations_lock:
            async with self._lock:
                reservation = SharedLockReservation(lock=self)

            try:
                yield reservation
            finally:
                reservation.release()

    def locked(self) -> bool:
        return self._lock.locked()

    def locked_reservations(self) -> bool:
        return self.locked() or self._reservations_lock.locked()

    async def _acquire(self, reservation: object) -> None:
        if self._locker is reservation:
            return

        await self._lock.acquire()
        self._locker = reservation

    def _release(self, reservation: object) -> None:
        if self._locker is not reservation:
            return

        self._locker = None
        self._lock.release()


class SharedLockReservation:
    def __init__(self, *, lock: SharedLock) -> None:
        self._lock = lock

    async def acquire(self) -> None:
        """Lock the queue, blocking any other reservation or lock itself if the
        queue is already locked.
        Return True if the lock don't block itself, False otherwise.
        """
        await self._lock._acquire(self)

    def locked(self) -> bool:
        return self._lock._locker is self

    def release(self) -> None:
        self._lock._release(self)


class Cancellable(t.Generic[R]):
    def __init__(self, func: t.Callable[..., t.Coroutine[t.Any, t.Any, R]]) -> None:
        self.func = func
        self._tasks: set[asyncio.Task] = set()

    def cancel(self) -> None:
        for task in self._tasks:
            task.cancel()

    async def __call__(self, *args: t.Any, **kwargs: t.Any) -> R:
        task: asyncio.Task = asyncio.create_task(
            self.func(*args, **kwargs), name=f"cancellable({self.func})"
        )
        self._tasks.add(task)
        try:
            return await task
        finally:
            self._tasks.remove(task)
