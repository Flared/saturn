import datetime

import asyncstdlib as alib
import pytest

from saturn_engine.worker.inventories import PeriodicInventory
from tests.conftest import FreezeTime
from tests.utils import TimeForwardLoop


@pytest.mark.asyncio
async def test_periodic_inventory(
    frozen_time: FreezeTime,
    event_loop: TimeForwardLoop,
) -> None:
    start_time = event_loop.time()
    start_date = datetime.datetime.utcfromtimestamp(event_loop.time())

    last_week = start_date - datetime.timedelta(days=7)

    inventory = PeriodicInventory.from_options(
        {
            "start_date": last_week.isoformat(),
            "interval": "@daily",
        },
    )

    # The first batch should be from last week to now.
    iterator = inventory.iterate()
    for expected_id in [
        "1969-12-26T00:00:00",
        "1969-12-27T00:00:00",
        "1969-12-28T00:00:00",
        "1969-12-29T00:00:00",
        "1969-12-30T00:00:00",
        "1969-12-31T00:00:00",
        "1970-01-01T00:00:00",
    ]:
        assert (await iterator.__anext__()).id == expected_id

    # Didn't need to wait.
    assert event_loop.time() == start_time

    # This "time" we needed to sleep. TIME. You get it?
    assert (await iterator.__anext__()).id == "1970-01-02T00:00:00"
    assert (
        datetime.datetime.utcfromtimestamp(event_loop.time()).isoformat()
        == "1970-01-02T00:00:00"
    )


@pytest.mark.asyncio
async def test_periodic_end_date(
    frozen_time: FreezeTime,
    event_loop: TimeForwardLoop,
) -> None:
    start_date = datetime.datetime.utcfromtimestamp(event_loop.time())
    last_week = start_date - datetime.timedelta(days=7)
    yesterday = start_date - datetime.timedelta(days=1)

    inventory = PeriodicInventory.from_options(
        {
            "start_date": last_week.isoformat(),
            "end_date": yesterday.isoformat(),
            "interval": "@daily",
        },
    )

    batch_ids = [i.id for i in await alib.list(inventory.iterate())]
    assert batch_ids == [
        "1969-12-26T00:00:00",
        "1969-12-27T00:00:00",
        "1969-12-28T00:00:00",
        "1969-12-29T00:00:00",
        "1969-12-30T00:00:00",
        "1969-12-31T00:00:00",
    ]
