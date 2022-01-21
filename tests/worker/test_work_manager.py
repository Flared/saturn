from unittest.mock import Mock

import pytest

from saturn_engine.core.api import InventoryItem
from saturn_engine.core.api import LockResponse
from saturn_engine.core.api import PipelineInfo
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import QueuePipeline
from saturn_engine.core.api import ResourceItem
from saturn_engine.core.api import TopicItem
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
    worker_manager_client.lock.return_value = LockResponse(items=[], resources=[])

    work_sync = await work_manager.sync()
    assert work_sync == WorkSync.empty()

    # Sync add 3 new items.
    worker_manager_client.lock.return_value = LockResponse(
        items=[
            QueueItem(
                name="q1",
                input=InventoryItem(
                    name="t1", type="DummyInventory", options={"count": 1000}
                ),
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                output={},
            ),
            QueueItem(
                name="q2",
                input=TopicItem(
                    name="t2",
                    type="DummyTopic",
                ),
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                output={},
            ),
            QueueItem(
                name="q3",
                input=TopicItem(
                    name="t3",
                    type="DummyTopic",
                ),
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                output={},
            ),
        ],
        resources=[
            ResourceItem(name="r1", type="FakeResource", data={"foo": "bar"}),
            ResourceItem(name="r2", type="FakeResource", data={"foo": "biz"}),
        ],
    )

    work_sync = await work_manager.sync()
    assert len(work_sync.queues.add) == 3
    assert len(work_sync.resources.add) == 2
    assert work_sync.queues.drop == []
    assert work_sync.resources.drop == []

    q2_work = work_manager.work_queue_by_name("q1")
    q3_work = work_manager.work_queue_by_name("q3")
    r2_resource = work_manager.worker_resources["r2"]

    # New sync add 1 and drop 2 items.
    worker_manager_client.lock.return_value = LockResponse(
        items=[
            QueueItem(
                name="q2",
                input=TopicItem(
                    name="t2",
                    type="DummyTopic",
                ),
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                output={},
            ),
            QueueItem(
                name="q4",
                input=TopicItem(
                    name="t4",
                    type="DummyTopic",
                ),
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                output={},
            ),
        ],
        resources=[
            ResourceItem(name="r1", type="FakeResource", data={"foo": "bar"}),
        ],
    )

    work_sync = await work_manager.sync()
    assert len(work_sync.queues.add) == 1
    assert len(work_sync.resources.add) == 0
    # Ensure the item dropped are the same queue object that were added.
    assert set(work_sync.queues.drop) == {q2_work, q3_work}
    assert set(work_sync.resources.drop) == {r2_resource}
