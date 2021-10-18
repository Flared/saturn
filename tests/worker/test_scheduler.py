import asyncio
from collections import Counter
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import sentinel

import asyncstdlib as alib
import pytest

from saturn_engine.core import Message
from saturn_engine.worker.queues import Queue
from saturn_engine.worker.scheduler import Scheduler


@pytest.fixture
async def scheduler(event_loop: asyncio.AbstractEventLoop) -> AsyncIterator[Scheduler]:
    _scheduler = Scheduler()
    yield _scheduler
    await _scheduler.close()


@pytest.mark.asyncio
async def test_scheduler(
    scheduler: Scheduler, event_loop: asyncio.AbstractEventLoop
) -> None:
    queue1 = MagicMock(spec=Queue)
    queue1.run.return_value = alib.cycle([sentinel.queue1])
    queue2 = MagicMock(spec=Queue)
    queue2.run.return_value = alib.cycle([sentinel.queue2])

    scheduler.add(queue1)
    scheduler.add(queue2)

    messages: Counter[Message] = Counter()
    async with alib.scoped_iter(scheduler.run()) as generator:
        async for item in alib.islice(generator, 10):
            messages[item] += 1
        assert messages == {sentinel.queue1: 5, sentinel.queue2: 5}

        # Removing a queue should cancel its task.
        await scheduler.remove(queue2)

        messages.clear()
        async for item in alib.islice(generator, 10):
            messages[item] += 1

        assert messages == {sentinel.queue1: 10}

        # Adding new queue adds it to the loop.
        queue3 = MagicMock(spec=Queue)
        queue3.run.return_value = alib.cycle([sentinel.queue3])
        scheduler.add(queue3)

        messages.clear()
        async for item in alib.islice(generator, 10):
            messages[item] += 1
        assert messages == {sentinel.queue1: 5, sentinel.queue3: 5}


@pytest.mark.asyncio
async def test_scheduler_queue_errors(scheduler: Scheduler) -> None:
    queue1 = MagicMock(spec=Queue)
    queue1.run.return_value = alib.cycle([sentinel.queue1])
    scheduler.add(queue1)

    messages: Counter[Message] = Counter()

    # Add bogus queues that raise exceptions.
    async def queue_error_init() -> AsyncGenerator:
        raise ValueError
        while True:
            yield

    async def queue_error_loop() -> AsyncGenerator:
        while True:
            yield sentinel.queue3
            raise ValueError

    queue2 = MagicMock(spec=Queue)
    queue2.run.side_effect = queue_error_init
    scheduler.add(queue2)

    queue3 = MagicMock(spec=Queue)
    queue3.run.side_effect = queue_error_loop
    scheduler.add(queue3)

    async with alib.scoped_iter(scheduler.run()) as generator:

        messages.clear()
        async for item in alib.islice(generator, 5):
            messages[item] += 1
        assert messages == {sentinel.queue1: 4, sentinel.queue3: 1}

        # Add a queue that closes.
        async def queue_closing() -> AsyncGenerator:
            while False:
                yield

        queue4 = MagicMock(spec=Queue)
        queue4.run.side_effect = queue_error_init
        scheduler.add(queue4)

        async for item in alib.islice(generator, 10):
            pass

        assert queue4 not in scheduler.queues


@pytest.mark.asyncio
async def test_scheduler_close_error(scheduler: Scheduler) -> None:
    close_mock = Mock()

    async def queue_error_close() -> AsyncGenerator:
        try:
            while True:
                yield
        except GeneratorExit:
            close_mock()
            raise ValueError from None

    queue = MagicMock(spec=Queue)
    queue.run.side_effect = queue_error_close
    scheduler.add(queue)
    async for item in alib.islice(scheduler.run(), 10):
        pass
    await scheduler.close()
    close_mock.assert_called_once()
