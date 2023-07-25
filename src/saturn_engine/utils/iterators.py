import typing as t

import asyncio

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
