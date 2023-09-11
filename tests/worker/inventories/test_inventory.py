import typing as t

import json
from datetime import timedelta

import asyncstdlib as alib
import pytest

from saturn_engine.core import Cursor
from saturn_engine.core import MessageId
from saturn_engine.utils import utcnow
from saturn_engine.worker.inventories.static import StaticInventory
from saturn_engine.worker.inventory import CursorsState
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

    async def next_batch(self, after: t.Optional[Cursor] = None) -> list[Item]:
        if self.retried == self.retrying_count:
            return [Item(id=MessageId("1"), args={"x": 1})]
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


async def test_legacy_cursor() -> None:
    inventory = StaticInventory(
        options=StaticInventory.Options(items=[{"x": 1}, {"x": 2}])
    )
    items = await alib.list(inventory.run(after=Cursor("0")))
    assert [i.args for i in items] == [{"x": 2}]
    assert (c := inventory.cursor)
    assert json.loads(c) == {"v": 1, "a": "0"}

    async with items[0].context:
        pass

    assert (c := inventory.cursor)
    assert json.loads(c) == {"v": 1, "a": "1"}


async def test_skip_partials() -> None:
    inventory = StaticInventory(
        options=StaticInventory.Options(items=[{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}])
    )
    cursor = CursorsState(after=Cursor("0"), partials={Cursor("2")}).as_cursor()
    items = await alib.list(inventory.run(after=cursor))
    for item in items:
        async with item:
            pass

    assert [i.args for i in items] == [{"x": 2}, {"x": 4}]
    assert (c := inventory.cursor)
    assert json.loads(c) == {"v": 1, "a": "3"}


async def test_max_partials() -> None:
    inventory = StaticInventory(
        options=StaticInventory.Options(
            items=[{"x": 1}, {"x": 2}, {"x": 3}, {"x": 4}, {"x": 5}]
        )
    )
    inventory.max_partials = 2
    cursor = CursorsState(after=Cursor("0"), partials={Cursor("5")}).as_cursor()
    items = await alib.list(inventory.run(after=cursor))
    for item in items[1:]:
        async with item:
            pass
    assert [i.args for i in items] == [{"x": 2}, {"x": 3}, {"x": 4}, {"x": 5}]
    assert (c := inventory.cursor)
    assert json.loads(c) == {"v": 1, "a": "0", "p": ["2", "3"]}
