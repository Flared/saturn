import typing as t

import asyncio
import dataclasses
from collections import Counter
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from unittest.mock import Mock
from unittest.mock import sentinel

import asyncstdlib as alib
import pytest

from saturn_engine.utils.asyncutils import aiter2agen
from saturn_engine.worker.executors.scheduler import Schedulable
from saturn_engine.worker.executors.scheduler import Scheduler


@pytest.fixture
async def scheduler(
    event_loop: asyncio.AbstractEventLoop,
) -> AsyncIterator[Scheduler[object]]:
    _scheduler: Scheduler[object] = Scheduler()
    yield _scheduler
    await _scheduler.close()


T = t.TypeVar("T")


@dataclasses.dataclass(eq=False)
class SimpleSchedulable(t.Generic[T]):
    iterable: AsyncGenerator[T, None]
    name: str


def make_schedulable(iterable: AsyncGenerator[T, None]) -> Schedulable[T]:
    return SimpleSchedulable(iterable=iterable, name="")


@pytest.mark.asyncio
async def test_scheduler(
    scheduler: Scheduler[object], event_loop: asyncio.AbstractEventLoop
) -> None:
    schedulable1 = make_schedulable(
        iterable=aiter2agen(alib.cycle([sentinel.schedulable1]))
    )
    schedulable2 = make_schedulable(
        iterable=aiter2agen(alib.cycle([sentinel.schedulable2]))
    )

    scheduler.add(schedulable1)
    scheduler.add(schedulable2)

    messages: Counter[object] = Counter()
    async with alib.scoped_iter(scheduler.run()) as generator:
        async for item in alib.islice(generator, 10):
            messages[item] += 1
        assert messages == {sentinel.schedulable1: 5, sentinel.schedulable2: 5}

        # Removing an item should cancel its task.
        await scheduler.remove(schedulable2)

        messages.clear()
        async for item in alib.islice(generator, 10):
            messages[item] += 1

        assert messages == {sentinel.schedulable1: 10}

        # Adding newn item adds it to the loop.
        schedulable3 = make_schedulable(
            iterable=aiter2agen(alib.cycle([sentinel.schedulable3]))
        )
        scheduler.add(schedulable3)

        messages.clear()
        async for item in alib.islice(generator, 10):
            messages[item] += 1
        assert messages == {sentinel.schedulable1: 5, sentinel.schedulable3: 5}


# TODO: Reenable once we don't cause fatal error in scheduler.
@pytest.mark.skip
@pytest.mark.asyncio
async def test_scheduler_iter_errors(scheduler: Scheduler) -> None:
    schedulable1 = make_schedulable(
        iterable=aiter2agen(alib.cycle([sentinel.schedulable1]))
    )
    scheduler.add(schedulable1)

    messages: Counter[object] = Counter()

    # Add bogus iterable that raise exceptions.
    async def error_init() -> AsyncGenerator:
        raise ValueError
        while True:
            yield

    async def error_loop() -> AsyncGenerator:
        while True:
            yield sentinel.schedulable3
            raise ValueError

    schedulable2 = make_schedulable(iterable=error_init())
    scheduler.add(schedulable2)

    schedulable3 = make_schedulable(iterable=error_loop())
    scheduler.add(schedulable3)

    async with alib.scoped_iter(scheduler.run()) as generator:
        messages.clear()
        async for item in alib.islice(generator, 5):
            messages[item] += 1
        assert messages == {sentinel.schedulable1: 4, sentinel.schedulable3: 1}

        # Add an item that closes.
        async def closing() -> AsyncGenerator:
            while False:
                yield

        schedulable4 = make_schedulable(iterable=closing())
        scheduler.add(schedulable4)

        async for item in alib.islice(generator, 10):
            pass

        assert schedulable4 not in scheduler.schedule_slots


@pytest.mark.asyncio
async def test_scheduler_close_error(scheduler: Scheduler) -> None:
    close_mock = Mock()

    async def error_close() -> AsyncGenerator:
        try:
            while True:
                yield sentinel.loop
        except GeneratorExit:
            close_mock()
            raise ValueError from None

    schedulable = make_schedulable(iterable=error_close())
    scheduler.add(schedulable)
    async for item in alib.islice(scheduler.run(), 10):
        pass
    await scheduler.close()
    close_mock.assert_called_once()
