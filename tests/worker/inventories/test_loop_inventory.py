import contextlib
import json

import asyncstdlib as alib

from saturn_engine.utils.asyncutils import TasksGroup
from saturn_engine.worker.executors.bootstrap import RemoteException
from saturn_engine.worker.inventories.loop import LoopInventory
from saturn_engine.worker.inventories.loop import StopLoopInventory
from tests.utils import TimeForwardLoop


async def test_loop_inventory() -> None:
    inventory = LoopInventory(options=LoopInventory.Options(max_iterations=20))

    async for i, item in alib.enumerate(inventory.run()):
        with contextlib.suppress(ValueError, StopLoopInventory):
            async with item:
                if item.args["iteration"] == 5:
                    raise ValueError("test")
                if item.args["iteration"] == 10:
                    raise StopLoopInventory()
            assert item.args["iteration"] == i

    assert (c := inventory.cursor)
    assert json.loads(c) == {"v": 1, "a": "10"}


async def test_loop_inventory_with_remove_exc() -> None:
    inventory = LoopInventory(options=LoopInventory.Options(max_iterations=20))

    async for i, item in alib.enumerate(inventory.run()):
        with contextlib.suppress(ValueError, RemoteException):
            async with item:
                if item.args["iteration"] == 1:
                    raise ValueError("test")
                try:
                    if item.args["iteration"] == 5:
                        raise ValueError("test")
                    if item.args["iteration"] == 10:
                        raise StopLoopInventory()
                except Exception as e:
                    raise RemoteException.from_exception(e) from None
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


async def test_loop_concurrency(event_loop: TimeForwardLoop) -> None:
    inventory = LoopInventory(options=LoopInventory.Options(max_iterations=20))

    async with alib.scoped_iter(inventory.run()) as run, TasksGroup() as group:
        item = await alib.anext(run)
        async with event_loop.until_idle():
            next_item = group.create_task(alib.anext(run))
        assert not next_item.done()

        async with event_loop.until_idle():
            async with item:
                pass
        assert next_item.done()
