from unittest.mock import Mock

import pytest

from saturn_engine.core.api import ComponentDefinition, QueueItemState
from saturn_engine.core.api import LockResponse
from saturn_engine.core.api import PipelineInfo
from saturn_engine.core.api import QueueItemWithState
from saturn_engine.core.api import QueuePipeline
from saturn_engine.core.api import ResourceItem
from saturn_engine.core.api import ResourcesProviderItem
from saturn_engine.core import JobId, Cursor
from saturn_engine.worker.work_manager import WorkManager
from saturn_engine.worker.work_manager import WorkSync


@pytest.mark.asyncio
async def test_sync(
    fake_resource_class: str,
    fake_resources_provider_class: str,
    fake_pipeline_info: PipelineInfo,
    work_manager: WorkManager,
    worker_manager_client: Mock,
) -> None:
    # Sync does nothing.
    worker_manager_client.lock.return_value = LockResponse(
        items=[], resources=[], resources_providers=[], executors=[]
    )

    work_sync = await work_manager.sync()
    assert work_sync == WorkSync.empty()

    # Sync add 3 new items.
    worker_manager_client.lock.return_value = LockResponse(
        items=[
            QueueItemWithState(
                name=JobId("q1"),
                input=ComponentDefinition(
                    name="t1", type="DummyInventory", options={"count": 1000}
                ),
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                labels={"owner": "team-saturn"},
                output={},
                executor="e1",
                state=QueueItemState(cursor=Cursor("1"))
            ),
            QueueItemWithState(
                name=JobId("q2"),
                input=ComponentDefinition(
                    name="t2",
                    type="DummyTopic",
                ),
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                labels={"owner": "team-saturn"},
                output={},
                executor="e1",
            ),
            QueueItemWithState(
                name=JobId("q3"),
                input=ComponentDefinition(
                    name="t3",
                    type="DummyTopic",
                ),
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                labels={"owner": "team-saturn"},
                output={},
                executor="e2",
            ),
        ],
        resources=[
            ResourceItem(name="r1", type="FakeResource", data={"foo": "bar"}),
            ResourceItem(name="r2", type="FakeResource", data={"foo": "biz"}),
        ],
        resources_providers=[
            ResourcesProviderItem(
                name="rp1",
                type=fake_resources_provider_class,
                resource_type="FakeResource",
                options={"foo": "bar"},
            ),
            ResourcesProviderItem(
                name="rp2",
                type=fake_resources_provider_class,
                resource_type="FakeResource",
                options={"foo": "biz"},
            ),
        ],
        executors=[
            ComponentDefinition(name="e1", type="FakeExecutor", options={"foo": "bar"}),
            ComponentDefinition(name="e2", type="FakeExecutor", options={"foo": "bar"}),
        ],
    )

    work_sync = await work_manager.sync()
    assert len(work_sync.queues.add) == 3
    assert len(work_sync.resources.add) == 2
    assert len(work_sync.resources_providers.add) == 2
    assert len(work_sync.executors.add) == 2
    assert work_sync.queues.drop == []
    assert work_sync.resources.drop == []
    assert work_sync.resources_providers.drop == []
    assert work_sync.executors.drop == []

    q2_work = work_manager.work_queue_by_name("q1")
    q3_work = work_manager.work_queue_by_name("q3")
    r2_resource = work_manager.worker_resources["r2"]
    rp2_resource_provider = work_manager.worker_resources_providers["rp2"]
    e2_executor = work_manager.worker_executors["e2"]

    # New sync add 1 and drop 2 items.
    worker_manager_client.lock.return_value = LockResponse(
        items=[
            QueueItemWithState(
                name=JobId("q2"),
                input=ComponentDefinition(
                    name="t2",
                    type="DummyTopic",
                ),
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                labels={"owner": "team-saturn"},
                output={},
                executor="e1",
            ),
            QueueItemWithState(
                name=JobId("q4"),
                input=ComponentDefinition(
                    name="t4",
                    type="DummyTopic",
                ),
                pipeline=QueuePipeline(
                    info=fake_pipeline_info,
                    args={},
                ),
                labels={"owner": "team-saturn"},
                output={},
                executor="e1",
            ),
        ],
        resources=[
            ResourceItem(name="r1", type="FakeResource", data={"foo": "bar"}),
        ],
        resources_providers=[
            ResourcesProviderItem(
                name="rp1",
                type=fake_resources_provider_class,
                resource_type="FakeResource",
                options={"foo": "bar"},
            ),
        ],
        executors=[
            ComponentDefinition(name="e1", type="FakeExecutor", options={"foo": "bar"}),
        ],
    )

    work_sync = await work_manager.sync()
    assert len(work_sync.queues.add) == 1
    assert len(work_sync.resources.add) == 0
    assert len(work_sync.resources_providers.add) == 0
    assert len(work_sync.executors.add) == 0
    # Ensure the item dropped are the same queue object that were added.
    assert set(work_sync.queues.drop) == {q2_work, q3_work}
    assert set(work_sync.resources.drop) == {r2_resource}
    assert set(work_sync.resources_providers.drop) == {rp2_resource_provider}
    assert set(e.name for e in work_sync.executors.drop) == {e2_executor.name}
