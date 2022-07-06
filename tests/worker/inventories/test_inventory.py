import typing as t

from datetime import timedelta

import pytest

from saturn_engine.utils import utcnow
from saturn_engine.worker.inventory import Inventory
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.inventory import MaxRetriesError
from saturn_engine.worker.inventory import RetryBatch
from tests.conftest import FreezeTime


class RetryingInventory(Inventory):
    def __init__(
        self, *, retry_delay: t.Optional[timedelta], retrying_count: int = 1
    ) -> None:
        self.retried = 0
        self.retry_delay = retry_delay
        self.retrying_count = retrying_count

    async def next_batch(self, after: t.Optional[str] = None) -> list[Item]:
        if self.retried == self.retrying_count:
            return [Item(id="1", args={"x": 1})]
        self.retried += 1
        raise RetryBatch(delay=self.retry_delay, max_retries=5)


@pytest.mark.asyncio
async def test_next_batch_retry(frozen_time: FreezeTime) -> None:
    start_date = utcnow()
    inventory = RetryingInventory(retry_delay=None)
    iterator = inventory.iterate()

    assert (await iterator.__anext__()).id == "1"

    # Didn't need to wait.
    assert utcnow() == start_date
    # And yet, it was retried.
    assert inventory.retried == 1


@pytest.mark.asyncio
async def test_next_batch_retry_with_delay(frozen_time: FreezeTime) -> None:
    start_date = utcnow()
    inventory = RetryingInventory(retry_delay=timedelta(seconds=10), retrying_count=2)
    iterator = inventory.iterate()

    assert (await iterator.__anext__()).id == "1"

    # Slept at least 10 seconds.
    assert utcnow() >= (start_date + timedelta(seconds=20))
    # And it was retried.
    assert inventory.retried == 2


@pytest.mark.asyncio
async def test_next_batch_max_retries(frozen_time: FreezeTime) -> None:
    start_date = utcnow()
    inventory = RetryingInventory(retry_delay=timedelta(seconds=10), retrying_count=10)
    iterator = inventory.iterate()

    with pytest.raises(MaxRetriesError):
        await iterator.__anext__()

    # Slept at least 50 seconds.
    assert utcnow() >= (start_date + timedelta(seconds=50))
    # And it was retried.
    assert inventory.retried == 6
