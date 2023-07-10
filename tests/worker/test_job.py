import typing as t

import asyncio
import dataclasses

import asyncstdlib as alib
import pytest

from saturn_engine.core import Cursor
from saturn_engine.core.api import QueueItemWithState
from saturn_engine.worker.inventory import Inventory
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.job import Job
from saturn_engine.worker.services.job_state.service import JobStateService
from saturn_engine.worker.services.manager import ServicesManager


class FakeInventory(Inventory):
    name = "fake_inventory"

    @dataclasses.dataclass
    class Options:
        data: list[int]

    def __init__(self, *args: t.Any, options: Options, **kwargs: t.Any) -> None:
        self.options = options

    async def next_batch(self, after: t.Optional[Cursor] = None) -> list[Item]:
        raise NotImplementedError()

    async def iterate(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        for i, x in enumerate(self.options.data):
            await asyncio.sleep(x)
            yield Item(id=str(i), args={"x": x})


@pytest.mark.asyncio
async def test_job_iteration(
    fake_queue_item: QueueItemWithState, services_manager: ServicesManager
) -> None:
    fake_queue_item.config["job"] = {
        "enable_cursors_states": True,
        "buffer_flush_after": 7,
        "buffer_size": 2,
    }
    inventory = FakeInventory(options=FakeInventory.Options(data=[5, 4, 3, 2, 1]))
    job = Job(
        inventory=inventory,
        queue_item=fake_queue_item,
        services=services_manager.services,
    )

    items = await alib.list(job.run())
    assert [i.id for i in items] == ["0", "1", "2", "3", "4"]  # type: ignore[union-attr]

    job_state_service = services_manager.services.cast_service(JobStateService)
    with job_state_service._store.flush() as state:
        assert len(state.jobs) == 1
        job_state = state.jobs[fake_queue_item.name]
        assert job_state.cursor == "4"
        assert job_state.completion
