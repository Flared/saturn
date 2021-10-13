from collections import Counter
from typing import Iterator
from unittest.mock import MagicMock
from unittest.mock import sentinel

import asyncstdlib as alib
import pytest

from saturn_engine.core import Message
from saturn_engine.worker.queues import Queue
from saturn_engine.worker.scheduler import Scheduler


@pytest.fixture
def scheduler() -> Iterator[Scheduler]:
    _scheduler = Scheduler()
    yield _scheduler
    _scheduler.close()


@pytest.mark.asyncio
async def test_scheduler(scheduler: Scheduler) -> None:
    queue1 = MagicMock(spec=Queue)
    queue1.get.return_value = sentinel.queue1
    queue2 = MagicMock(spec=Queue)
    queue2.get.return_value = sentinel.queue2

    scheduler.add(queue1)
    scheduler.add(queue2)

    messages: Counter[Message] = Counter()
    async for item, _ in alib.zip(scheduler.iter(), range(10)):
        messages[item] += 1
    assert messages == {sentinel.queue1: 5, sentinel.queue2: 5}

    # Removing a queue should cancel its task.
    scheduler.remove(queue2)

    async for item, _ in alib.zip(scheduler.iter(), range(10)):
        messages[item] += 1

    assert messages[sentinel.queue1] >= 14
    assert messages[sentinel.queue2] <= 6

    # Adding new queue adds it to the loop.
    queue3 = MagicMock(spec=Queue)
    queue3.get.return_value = sentinel.queue3
    scheduler.add(queue3)

    async for item, _ in alib.zip(scheduler.iter(), range(10)):
        messages[item] += 1
    assert messages[sentinel.queue1] >= 19
    assert messages[sentinel.queue3] == 5

    # Add new queue that raise an exception.
    queue4 = MagicMock(spec=Queue)
    queue4.get.side_effect = ValueError()
    scheduler.add(queue4)

    async for item, _ in alib.zip(scheduler.iter(), range(10)):
        messages[item] += 1
    assert messages[sentinel.queue1] >= 24
    assert messages[sentinel.queue3] == 10
