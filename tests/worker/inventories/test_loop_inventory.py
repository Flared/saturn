import contextlib
import json
from unittest.mock import Mock

import asyncstdlib as alib

from saturn_engine.utils.asyncutils import TasksGroup
from saturn_engine.worker.executors.bootstrap import RemoteException
from saturn_engine.worker.inventories.loop import LoopInventory
from saturn_engine.worker.inventories.loop import StopLoopEvent
from saturn_engine.worker.services.hooks import PipelineEventsEmitted
from tests.utils import TimeForwardLoop


async def test_loop_inventory() -> None:
    inventory = LoopInventory(options=LoopInventory.Options(max_iterations=20))

    async for i, item in alib.enumerate(inventory.run()):
        async with item:
            if item.args["iteration"] == 10:
                for cb in item.config["hooks"]["pipeline_events_emitted"]:
                    await cb(PipelineEventsEmitted(Mock(), [StopLoopEvent()]))
            assert item.args["iteration"] == i

    assert (c := inventory.cursor)
    assert json.loads(c) == {"v": 1, "a": "10"}


async def test_loop_inventory_max_iterations() -> None:
    inventory = LoopInventory(options=LoopInventory.Options(max_iterations=20))

    async for i, item in alib.enumerate(inventory.run()):
        with contextlib.suppress(ValueError, RemoteException):
            async with item:
                pass
            assert item.args["iteration"] == i

    assert (c := inventory.cursor)
    assert json.loads(c) == {"v": 1, "a": "19"}


async def test_loop_concurrency(running_event_loop: TimeForwardLoop) -> None:
    inventory = LoopInventory(options=LoopInventory.Options(max_iterations=20))

    async with alib.scoped_iter(inventory.run()) as run, TasksGroup() as group:
        item = await alib.anext(run)
        async with running_event_loop.until_idle():
            next_item = group.create_task(alib.anext(run))
        assert not next_item.done()

        async with running_event_loop.until_idle():
            async with item:
                pass
        assert next_item.done()
