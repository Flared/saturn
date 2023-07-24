import typing as t

import asyncio

import asyncstdlib as alib
import pytest

from saturn_engine.utils import ExceptionGroup
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

    # Each time we flush a batch after a delay, we should start a new batch.
    iterator = generator([0, 3, 3, 0, 0, 40])
    buf_it = iterators.async_buffered(iterator, flush_after=5, buffer_size=3)
    items = await alib.list(buf_it)
    assert items == [[0, 3], [3, 0, 0], [40]]


async def test_flatten() -> None:
    iterator = alib.iter([[1, 2], [3, 4], [5]])
    flatten_it = iterators.async_flatten(iterator)
    items = await alib.list(flatten_it)
    assert items == [1, 2, 3, 4, 5]


async def test_fanin() -> None:
    async def fast() -> t.AsyncIterator[int]:
        for i in range(5):
            await asyncio.sleep(1.1)
            yield i

    async def slow() -> t.AsyncIterator[int]:
        for i in range(10, 14):
            await asyncio.sleep(2)
            yield i

    assert await alib.list(iterators.fanin(fast(), slow())) == [
        0,
        10,
        1,
        2,
        11,
        3,
        4,
        12,
        13,
    ]


async def test_fanin_fails() -> None:
    async def nosleep() -> t.AsyncIterator[int]:
        for i in range(5):
            yield i

    async def fail() -> t.AsyncIterator[int]:
        yield 100
        raise ValueError("Fail")

    results = []
    with pytest.raises(ExceptionGroup):
        async for x in iterators.fanin(nosleep(), fail(), nosleep()):
            results.append(x)

    assert list(sorted(results)) == [0, 0, 1, 1, 100]
