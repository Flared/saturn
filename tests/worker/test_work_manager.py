from unittest.mock import Mock

import pytest

from saturn_engine.core.api import DummyItem
from saturn_engine.core.api import PipelineInfo
from saturn_engine.core.api import QueuePipeline
from saturn_engine.core.api import ResourceItem
from saturn_engine.core.api import SyncResponse
from saturn_engine.utils import flatten
from saturn_engine.worker.work_manager import WorkManager
from saturn_engine.worker.work_manager import WorkSync


@pytest.mark.asyncio
async def test_sync(
    fake_resource_class: str,
    fake_pipeline_info: PipelineInfo,
    work_manager: WorkManager,
    worker_manager_client: Mock,
) -> None:
    # Sync does nothing.
    worker_manager_client.sync.return_value = SyncResponse(items=[], resources=[])

    work_sync = await work_manager.sync()
    assert work_sync == WorkSync.empty()

    # Sync add 3 new items.
    worker_manager_client.sync.return_value = SyncResponse(
        items=[
            DummyItem(
                id="q1",
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                options={
                    "tasks_count": 2,
                    "queues_count": 3,
                },
            ),
            DummyItem(
                id="q2",
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                options={},
            ),
            DummyItem(
                id="q3",
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                options={},
            ),
        ],
        resources=[
            ResourceItem(id="r1", type="FakeResource", data={"foo": "bar"}),
            ResourceItem(id="r2", type="FakeResource", data={"foo": "biz"}),
        ],
    )

    work_sync = await work_manager.sync()
    assert len(work_sync.queues.add) == 5
    assert len(work_sync.tasks.add) == 2
    assert len(work_sync.resources.add) == 2
    assert work_sync.queues.drop == []
    assert work_sync.tasks.drop == []
    assert work_sync.resources.drop == []

    q2_work = work_manager.work_items_by_id("q1")
    q3_work = work_manager.work_items_by_id("q3")
    r2_resource = work_manager.worker_resources["r2"]

    # New sync add 1 and drop 2 items.
    worker_manager_client.sync.return_value = SyncResponse(
        items=[
            DummyItem(
                id="q2",
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                options={},
            ),
            DummyItem(
                id="q4",
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                options={},
            ),
        ],
        resources=[
            ResourceItem(id="r1", type="FakeResource", data={"foo": "bar"}),
        ],
    )

    work_sync = await work_manager.sync()
    assert len(work_sync.queues.add) == 1
    assert len(work_sync.resources.add) == 0
    # Ensure the item dropped are the same queue object that were added.
    assert set(work_sync.queues.drop) == set(flatten([q2_work.queues, q3_work.queues]))
    assert set(work_sync.tasks.drop) == set(flatten([q2_work.tasks, q3_work.tasks]))
    assert set(work_sync.resources.drop) == {r2_resource}
