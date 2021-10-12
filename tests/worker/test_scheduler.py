from collections import Counter
from unittest.mock import MagicMock
from unittest.mock import sentinel

import asyncstdlib as alib
import pytest

from saturn.core import Message
from saturn.worker.queues import Queue
from saturn.worker.scheduler import Scheduler


@pytest.mark.asyncio
async def test_scheduler() -> None:
    queue1 = MagicMock(spec=Queue)
    queue1.get.return_value = sentinel.queue1
    queue2 = MagicMock(spec=Queue)
    queue2.get.return_value = sentinel.queue2
    queue3 = MagicMock(spec=Queue)
    queue3.get.return_value = sentinel.queue3
    scheduler = Scheduler()

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
    scheduler.add(queue3)

    async for item, _ in alib.zip(scheduler.iter(), range(10)):
        messages[item] += 1
    assert messages[sentinel.queue1] >= 19
    assert messages[sentinel.queue3] == 5
