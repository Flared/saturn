import typing as t

import asyncio

import asyncstdlib as alib

T = t.TypeVar("T")


async def async_buffered(
    iterator: t.AsyncIterator[T],
    *,
    buffer_size: t.Optional[int] = None,
    flush_after: t.Optional[float] = None,
) -> t.AsyncIterator[list[T]]:
    items = []

    async def next_slice() -> None:
        async for item in alib.islice(alib.borrow(iterator), buffer_size):
            items.append(item)

    pending = {asyncio.create_task(next_slice())}

    while True:
        items.clear()
        done, pending = await asyncio.wait(pending, timeout=flush_after)

        if done:
            if not items:
                break
            pending = {asyncio.create_task(next_slice())}

        yield items.copy()


async def async_flatten(
    iterator: t.AsyncIterator[list[T]],
) -> t.AsyncIterator[T]:
    async for items in iterator:
        for item in items:
            yield item
