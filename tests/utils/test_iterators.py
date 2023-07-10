import typing as t

import asyncio

import asyncstdlib as alib

from saturn_engine.utils import iterators


async def test_buffered() -> None:
    # No param: Buffer the whole iterator.
    iterator = alib.iter([1, 2, 3, 4, 5])
    buf_it = iterators.async_buffered(iterator)
    items = await alib.list(buf_it)
    assert items == [[1, 2, 3, 4, 5]]

    # `buffer_size` chunk into at most N items.
    iterator = alib.iter([1, 2, 3, 4, 5])
    buf_it = iterators.async_buffered(iterator, buffer_size=2)
    items = await alib.list(buf_it)
    assert items == [[1, 2], [3, 4], [5]]

    # `flush_after` wait at most N seconds before flushing.
    async def generator(xs: list[int]) -> t.AsyncIterator[int]:
        for x in xs:
            await asyncio.sleep(x)
            yield x

    iterator = generator([1, 2, 3, 4, 5, 3])
    buf_it = iterators.async_buffered(iterator, flush_after=7)
    items = await alib.list(buf_it)
    assert items == [[1, 2, 3], [4], [5, 3]]

    # Using all params return as soon one condition is true.
    iterator = generator([1, 2, 3, 4, 5, 6])
    buf_it = iterators.async_buffered(iterator, flush_after=8, buffer_size=2)
    items = await alib.list(buf_it)
    assert items == [[1, 2], [3, 4], [5], [6]]


async def test_flatten() -> None:
    iterator = alib.iter([[1, 2], [3, 4], [5]])
    flatten_it = iterators.async_flatten(iterator)
    items = await alib.list(flatten_it)
    assert items == [1, 2, 3, 4, 5]
