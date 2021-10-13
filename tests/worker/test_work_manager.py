from unittest.mock import Mock
from unittest.mock import create_autospec

import pytest

from saturn_engine.client.worker_manager import JobItem
from saturn_engine.client.worker_manager import QueueItem
from saturn_engine.client.worker_manager import SyncResponse
from saturn_engine.client.worker_manager import WorkerManagerClient
from saturn_engine.utils import flatten
from saturn_engine.worker.work_manager import WorkManager


@pytest.fixture
def worker_manager_client() -> Mock:
    _worker_manager_client = create_autospec(WorkerManagerClient, instance=True)
    _worker_manager_client.sync.return_value = SyncResponse(items=[])
    return _worker_manager_client


@pytest.fixture
def work_manager(worker_manager_client: WorkerManagerClient) -> WorkManager:
    _work_manager = WorkManager(client=worker_manager_client)
    return _work_manager


@pytest.mark.asyncio
async def test_sync_queues(
    work_manager: WorkManager, worker_manager_client: Mock
) -> None:
    # Sync does nothing.
    worker_manager_client.sync.return_value = SyncResponse(items=[])

    queues_sync = await work_manager.sync_queues()
    assert queues_sync.add == []
    assert queues_sync.drop == []

    # Sync add 3 new items.
    worker_manager_client.sync.return_value = SyncResponse(
        items=[
            QueueItem(id="q1", pipeline="p1", ressources=[]),
            QueueItem(id="q2", pipeline="p2", ressources=[]),
            JobItem(id="q3", pipeline="p3", ressources=[], inventory="i4"),
        ]
    )

    queues_sync = await work_manager.sync_queues()
    assert len(queues_sync.add) == 3
    assert queues_sync.drop == []

    q2_queues = work_manager.queues_by_id("q1")
    q3_queues = work_manager.queues_by_id("q3")

    # New sync add 1 and drop 2 items.
    worker_manager_client.sync.return_value = SyncResponse(
        items=[
            QueueItem(id="q2", pipeline="p2", ressources=[]),
            JobItem(id="q4", pipeline="p4", ressources=[], inventory="i4"),
        ]
    )

    queues_sync = await work_manager.sync_queues()
    assert len(queues_sync.add) == 1
    # Ensure the item dropped are the same queue object that were added.
    assert set(queues_sync.drop) == set(flatten([q2_queues, q3_queues]))
