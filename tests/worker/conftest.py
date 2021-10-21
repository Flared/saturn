from collections.abc import AsyncIterator
from collections.abc import Iterator
from unittest.mock import Mock
from unittest.mock import create_autospec

import pytest

from saturn_engine.client.worker_manager import WorkerManagerClient
from saturn_engine.core.api import SyncResponse
from saturn_engine.worker.broker import Broker
from saturn_engine.worker.broker import ExecutorInit
from saturn_engine.worker.broker import WorkManagerInit
from saturn_engine.worker.context import Context
from saturn_engine.worker.executors import Executor
from saturn_engine.worker.queues.memory import reset as reset_memory_queues
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.work_manager import WorkManager


@pytest.fixture
def context(services_manager: ServicesManager) -> Iterator[Context]:
    yield Context(services=services_manager)


@pytest.fixture
def worker_manager_client() -> Mock:
    _worker_manager_client = create_autospec(WorkerManagerClient, instance=True)
    _worker_manager_client.sync.return_value = SyncResponse(items=[])
    return _worker_manager_client


@pytest.fixture
def work_manager(work_manager_maker: WorkManagerInit, context: Context) -> WorkManager:
    return work_manager_maker(context=context)


@pytest.fixture
def work_manager_maker(worker_manager_client: WorkerManagerClient) -> WorkManagerInit:
    def maker(context: Context) -> WorkManager:
        return WorkManager(context=context, client=worker_manager_client)

    return maker


@pytest.fixture
async def services_manager() -> AsyncIterator[ServicesManager]:
    _services_manager = ServicesManager()
    yield _services_manager
    await _services_manager.close()


@pytest.fixture
async def executor_maker() -> ExecutorInit:
    def maker() -> Executor:
        return create_autospec(Executor, instance=True)

    return maker


@pytest.fixture
async def broker(
    work_manager_maker: WorkManagerInit, executor_maker: ExecutorInit
) -> AsyncIterator[Broker]:
    _broker = Broker(work_manager=work_manager_maker, executor=executor_maker)
    yield _broker
    _broker.stop()
    await _broker.close()


@pytest.fixture(autouse=True)
def cleanup_memory_queues() -> Iterator[None]:
    yield
    reset_memory_queues()
