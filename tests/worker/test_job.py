import typing as t

import dataclasses
import json
from contextlib import AsyncExitStack

import asyncstdlib as alib
import pytest

from saturn_engine.core.api import QueueItemWithState
from saturn_engine.worker.executors.executable import ExecutableQueue
from saturn_engine.worker.inventory import Cursor
from saturn_engine.worker.inventory import Inventory
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.job import Job
from saturn_engine.worker.services.job_state.service import JobStateService
from saturn_engine.worker.services.manager import ServicesManager


class FakeInventory(Inventory):
    name = "fake_inventory"

    @dataclasses.dataclass
    class Options:
        items: list[Item]

    def __init__(self, *args: t.Any, options: Options, **kwargs: t.Any) -> None:
        self.options = options

    async def next_batch(self, after: t.Optional[Cursor] = None) -> list[Item]:
        raise NotImplementedError()

    async def iterate(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        for item in self.options.items:
            yield item


@pytest.mark.asyncio
async def test_inventory_set_cursor(
    services_manager: ServicesManager,
    fake_queue_item: QueueItemWithState,
    executable_queue_maker: t.Callable[..., ExecutableQueue],
) -> None:
    inventory = FakeInventory(
        options=FakeInventory.Options(
            items=[
                Item(cursor=Cursor("1"), args={"x": 1}),
                Item(cursor=Cursor("2"), args={"x": 1}),
            ]
        )
    )
    job_id = fake_queue_item.name
    job_state_store = services_manager.services.cast_service(JobStateService)._store
    job = Job(
        inventory=inventory,
        queue_item=fake_queue_item,
        services=services_manager.services,
    )
    xqueue = executable_queue_maker(definition=fake_queue_item, topic=job)

    async for xmsg in xqueue.run():
        async with xmsg._context:
            pass

    assert (cursor := job_state_store.job_state(job_id).cursor)
    assert json.loads(cursor) == {"v": 1, "a": "2"}


@pytest.mark.asyncio
async def test_inventory_set_cursor_after_completed(
    services_manager: ServicesManager,
    fake_queue_item: QueueItemWithState,
    executable_queue_maker: t.Callable[..., ExecutableQueue],
) -> None:
    def fail() -> None:
        raise ValueError()

    failing_stack = AsyncExitStack()
    failing_stack.callback(fail)
    inventory = FakeInventory(
        options=FakeInventory.Options(
            items=[
                Item(cursor=Cursor("0"), args={"x": 1}),
                Item(cursor=None, args={"x": 1}),
                Item(cursor=Cursor("2"), args={"x": 1}, context=failing_stack),
                Item(cursor=None, args={"x": 1}),
                Item(cursor=Cursor("4"), args={"x": 1}),
                Item(cursor=Cursor("5"), args={"x": 1}),
                Item(cursor=Cursor("6"), args={"x": 1}),
            ]
        )
    )
    job_id = fake_queue_item.name
    job_state_store = services_manager.services.cast_service(JobStateService)._store
    job = Job(
        inventory=inventory,
        queue_item=fake_queue_item,
        services=services_manager.services,
    )
    xqueue = executable_queue_maker(definition=fake_queue_item, topic=job)

    xmsg_ctxs: list[AsyncExitStack] = []
    async with alib.scoped_iter(xqueue.run()) as xrun:
        async for xmsg in alib.islice(xrun, 7):
            async with AsyncExitStack() as stack:
                await stack.enter_async_context(xmsg._context)
                xmsg_ctxs.append(stack.pop_all())

        assert job_state_store.job_state(job_id).cursor is None
        assert len(xmsg_ctxs) == 7

        # .: Pending, R: Ready
        #    |0|1|2|3|4|5|6|
        # -> |.|.|R|.|.|R|.|
        #    Nothing commited.
        with pytest.raises(ValueError):
            await xmsg_ctxs[2].aclose()
        await xmsg_ctxs[5].aclose()
        assert (cursor := job_state_store.job_state(job_id).cursor)
        assert json.loads(cursor) == {"v": 1, "p": ["2", "5"]}

        # .: Pending, R: Ready
        #    |0|1|2|3|4|5|6|
        # -> |C|.|R|R|.|R|.|
        #    Message 0 is commited.
        await xmsg_ctxs[3].aclose()
        await xmsg_ctxs[0].aclose()
        assert (cursor := job_state_store.job_state(job_id).cursor)
        assert json.loads(cursor) == {"v": 1, "a": "0", "p": ["2", "5"]}

        # .: Pending, R: Ready
        #    |0|1|2|3|4|5|6|
        # -> |C|R|C|R|.|R|.|
        #    Message 2 is commited (Message 3 has no cursor)
        await xmsg_ctxs[1].aclose()
        assert (cursor := job_state_store.job_state(job_id).cursor)
        assert json.loads(cursor) == {"v": 1, "a": "2", "p": ["5"]}

        # .: Pending, R: Ready
        #    |0|1|2|3|4|5|6|
        # -> |C|R|C|R|R|R|C|
        #    Message 6 is commited
        await xmsg_ctxs[6].aclose()
        await xmsg_ctxs[4].aclose()
        assert (cursor := job_state_store.job_state(job_id).cursor)
        assert json.loads(cursor) == {"v": 1, "a": "6"}
