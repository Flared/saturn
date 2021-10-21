from unittest.mock import Mock

import pytest

from saturn_engine.core.api import DummyItem
from saturn_engine.core.api import SyncResponse
from saturn_engine.utils import flatten
from saturn_engine.worker.work_manager import WorkManager
from saturn_engine.worker.work_manager import WorkSync


@pytest.mark.asyncio
async def test_sync(work_manager: WorkManager, worker_manager_client: Mock) -> None:
    # Sync does nothing.
    worker_manager_client.sync.return_value = SyncResponse(items=[])

    work_sync = await work_manager.sync()
    assert work_sync == WorkSync.empty()

    # Sync add 3 new items.
    worker_manager_client.sync.return_value = SyncResponse(
        items=[
            DummyItem(
                id="q1",
                pipeline="p1",
                ressources=[],
                options={
                    "tasks_count": 2,
                    "queues_count": 3,
                },
            ),
            DummyItem(id="q2", pipeline="p2", ressources=[], options={}),
            DummyItem(id="q3", pipeline="p3", ressources=[], options={}),
        ]
    )

    work_sync = await work_manager.sync()
    assert len(work_sync.queues.add) == 5
    assert len(work_sync.tasks.add) == 2
    assert work_sync.queues.drop == []
    assert work_sync.tasks.drop == []

    q2_work = work_manager.work_items_by_id("q1")
    q3_work = work_manager.work_items_by_id("q3")

    # New sync add 1 and drop 2 items.
    worker_manager_client.sync.return_value = SyncResponse(
        items=[
            DummyItem(id="q2", pipeline="p2", ressources=[], options={}),
            DummyItem(id="q4", pipeline="p4", ressources=[], options={}),
        ]
    )

    work_sync = await work_manager.sync()
    assert len(work_sync.queues.add) == 1
    # Ensure the item dropped are the same queue object that were added.
    assert set(work_sync.queues.drop) == set(flatten([q2_work.queues, q3_work.queues]))
    assert set(work_sync.tasks.drop) == set(flatten([q2_work.tasks, q3_work.tasks]))
