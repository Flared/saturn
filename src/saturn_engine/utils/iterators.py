import typing as t

import asyncio
import contextlib

import asyncstdlib as alib

from . import ExceptionGroup

T = t.TypeVar("T")


async def async_buffered(
    iterator: t.AsyncIterator[T],
    *,
    buffer_size: t.Optional[int] = None,
    flush_after: t.Optional[float] = None,
) -> t.AsyncIterator[list[T]]:
    items = []

    async def next_slice() -> None:
        async for item in iterator:
            items.append(item)
            if buffer_size is not None and len(items) >= buffer_size:
                break

    pending = {asyncio.create_task(next_slice())}

    while True:
        done, pending = await asyncio.wait(pending, timeout=flush_after)

        if done and not items:
            break

        yield_items = items.copy()
        items.clear()
        if done:
            pending = {asyncio.create_task(next_slice())}

        if yield_items:
            yield yield_items


async def async_flatten(
    iterator: t.AsyncIterator[list[T]],
) -> t.AsyncIterator[T]:
    async for items in iterator:
        for item in items:
            yield item


async def async_enter(
    iterator: t.AsyncIterator[t.AsyncContextManager[T]],
    *,
    error: t.Optional[t.Callable[[Exception], t.Awaitable]] = None,
) -> t.AsyncIterator[tuple[t.AsyncContextManager[T], T]]:
    async for context in iterator:
        try:
            item = await context.__aenter__()
        except Exception as e:
            if error:
                await error(e)
                continue
            raise
        yield (context, item)


@contextlib.asynccontextmanager
async def scoped_aiters(
    *iterators: t.AsyncIterator[T],
) -> t.AsyncIterator[list[t.AsyncIterator[T]]]:
    ctx = contextlib.AsyncExitStack()
    scoped_aiters = [
        await ctx.enter_async_context(alib.scoped_iter(i)) for i in iterators
    ]
    async with ctx:
        yield scoped_aiters


async def fanin(*iterators: t.AsyncIterator[T]) -> t.AsyncIterator[T]:
    anext_tasks: dict[asyncio.Task, t.AsyncIterator[T]] = {
        asyncio.create_task(alib.anext(i), name="fanin.anext"): i for i in iterators
    }
    errors: list[Exception] = []
    while True:
        if not anext_tasks:
            break

        done, _ = await asyncio.wait(
            anext_tasks.keys(), return_when=asyncio.FIRST_COMPLETED
        )
        for task in done:
            iterator = anext_tasks.pop(task)
            if task.cancelled():
                continue

            e = task.exception()
            if e is None:
                yield task.result()
            elif isinstance(e, StopAsyncIteration):
                continue
            elif isinstance(e, Exception):
                for task in anext_tasks:
                    if task not in done:
                        task.cancel()
                errors.append(e)
            else:
                raise e

            if not errors:
                anext_tasks[
                    asyncio.create_task(alib.anext(iterator), name="fanin.anext")
                ] = iterator

    if errors:
        raise ExceptionGroup("One iterator failed", errors)
