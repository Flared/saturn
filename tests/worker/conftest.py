from collections.abc import AsyncIterator
from collections.abc import Iterator
from typing import Callable
from typing import Optional
from unittest.mock import Mock
from unittest.mock import create_autospec

import asyncstdlib as alib
import pytest
from pytest_mock import MockerFixture

from saturn_engine.client.worker_manager import WorkerManagerClient
from saturn_engine.core import PipelineInfo
from saturn_engine.core import PipelineMessage
from saturn_engine.core import TopicMessage
from saturn_engine.core.api import SyncResponse
from saturn_engine.worker.broker import Broker
from saturn_engine.worker.broker import ExecutorInit
from saturn_engine.worker.broker import WorkManagerInit
from saturn_engine.worker.context import Context
from saturn_engine.worker.executable_message import ExecutableMessage
from saturn_engine.worker.executors import Executor
from saturn_engine.worker.parkers import Parkers
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


def pipeline() -> None:
    ...


@pytest.fixture
def fake_pipeline(mocker: MockerFixture) -> Iterator[Callable]:
    mock = mocker.patch(__name__ + ".pipeline", autospec=True)
    yield mock


@pytest.fixture
def fake_pipeline_info(fake_pipeline: Callable) -> PipelineInfo:
    return PipelineInfo.from_pipeline(fake_pipeline)


@pytest.fixture
def executable_maker(
    fake_pipeline_info: PipelineInfo,
) -> Callable[..., ExecutableMessage]:
    def maker(
        args: Optional[dict[str, object]] = None, parker: Optional[Parkers] = None
    ) -> ExecutableMessage:
        return ExecutableMessage(
            message=PipelineMessage(
                info=fake_pipeline_info,
                message=TopicMessage(args=args or {}),
            ),
            message_context=alib.nullcontext(),
            parker=parker or Parkers(),
        )

    return maker


@pytest.fixture(autouse=True)
def cleanup_memory_queues() -> Iterator[None]:
    yield
    reset_memory_queues()
