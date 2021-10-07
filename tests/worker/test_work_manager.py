from unittest.mock import Mock

import pytest

from saturn.client.worker_manager import SyncResponse
from saturn.client.worker_manager import WorkerManagerClient
from saturn.worker.work_manager import WorkManager


@pytest.fixture
def worker_manager_client() -> WorkerManagerClient:
    _worker_manager_client = Mock()
    _worker_manager_client.sync.return_value = SyncResponse(items=[])
    return _worker_manager_client


@pytest.fixture
def work_manager(worker_manager_client: WorkerManagerClient) -> WorkManager:
    _work_manager = WorkManager(client=worker_manager_client)
    return _work_manager


@pytest.mark.asyncio
async def test_sync_queues(work_manager: WorkManager) -> None:
    queues_sync = await work_manager.sync_queues()
    assert queues_sync.add == []
    assert queues_sync.keep == []
    assert queues_sync.drop == []
