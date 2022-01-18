from typing import Callable
from typing import Optional

import contextlib
import dataclasses
from collections.abc import AsyncIterator
from collections.abc import Iterator
from unittest.mock import Mock
from unittest.mock import create_autospec

import pytest

from saturn_engine.client.worker_manager import WorkerManagerClient
from saturn_engine.config import Config
from saturn_engine.core import PipelineInfo
from saturn_engine.core import Resource
from saturn_engine.core import TopicMessage
from saturn_engine.core.api import LockResponse
from saturn_engine.worker.broker import Broker
from saturn_engine.worker.broker import ExecutorInit
from saturn_engine.worker.broker import WorkManagerInit
from saturn_engine.worker.executable_message import ExecutableMessage
from saturn_engine.worker.executors import Executor
from saturn_engine.worker.executors import ExecutorManager
from saturn_engine.worker.parkers import Parkers
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.resources_manager import ResourcesManager
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.topics import Topic
from saturn_engine.worker.topics.memory import reset as reset_memory_queues
from saturn_engine.worker.work_manager import WorkManager


@pytest.fixture
def worker_manager_client() -> Mock:
    _worker_manager_client = create_autospec(WorkerManagerClient, instance=True)
    _worker_manager_client.lock.return_value = LockResponse(items=[], resources=[])
    return _worker_manager_client


@pytest.fixture
def work_manager(
    work_manager_maker: WorkManagerInit, services_manager: ServicesManager
) -> WorkManager:
    return work_manager_maker(services=services_manager.services)


@pytest.fixture
def work_manager_maker(
    worker_manager_client: WorkerManagerClient, services_manager: ServicesManager
) -> WorkManagerInit:
    def maker(services: Services = services_manager.services) -> WorkManager:
        return WorkManager(services=services, client=worker_manager_client)

    return maker


@pytest.fixture
async def services_manager(config: Config) -> AsyncIterator[ServicesManager]:
    _services_manager = ServicesManager(config)
    await _services_manager.open()
    yield _services_manager
    await _services_manager.close()


@pytest.fixture
def resources_manager() -> ResourcesManager:
    return ResourcesManager()


@pytest.fixture
def executor_maker() -> ExecutorInit:
    def maker(services: Services) -> Executor:
        return create_autospec(Executor, instance=True)

    return maker


@pytest.fixture
async def executor_manager_maker(
    resources_manager: ResourcesManager,
    executor_maker: ExecutorInit,
    services_manager: ServicesManager,
) -> AsyncIterator[Callable[..., ExecutorManager]]:
    async with contextlib.AsyncExitStack() as stack:

        def maker(
            executor: Optional[Executor] = None,
            concurrency: int = 5,
            services: Services = services_manager.services,
        ) -> ExecutorManager:
            executor = executor or executor_maker(services_manager.services)
            manager = ExecutorManager(
                resources_manager=resources_manager,
                executor=executor,
                concurrency=concurrency,
                services=services,
            )
            manager.start()
            stack.push_async_callback(manager.close)
            return manager

        yield maker


@pytest.fixture
async def broker_maker(
    work_manager_maker: WorkManagerInit, executor_maker: ExecutorInit, config: Config
) -> AsyncIterator[Callable[..., Broker]]:
    brokers = []

    def maker(
        work_manager: WorkManagerInit = work_manager_maker,
        executor: ExecutorInit = executor_maker,
    ) -> Broker:
        _broker = Broker(work_manager=work_manager, executor=executor, config=config)
        brokers.append(_broker)
        return _broker

    yield maker

    for _broker in brokers:
        await _broker.close()
        _broker.stop()


@pytest.fixture
async def broker(broker_maker: Callable[..., Broker]) -> AsyncIterator[Broker]:
    yield broker_maker()


@pytest.fixture
def executable_maker(
    fake_pipeline_info: PipelineInfo,
) -> Callable[..., ExecutableMessage]:
    def maker(
        args: Optional[dict[str, object]] = None,
        parker: Optional[Parkers] = None,
        pipeline_info: PipelineInfo = fake_pipeline_info,
        output: Optional[dict[str, list[Topic]]] = None,
    ) -> ExecutableMessage:
        return ExecutableMessage(
            message=PipelineMessage(
                info=pipeline_info or fake_pipeline_info,
                message=TopicMessage(args=args or {}),
            ),
            parker=parker or Parkers(),
            output=output or {},
        )

    return maker


@pytest.fixture(autouse=True)
def cleanup_memory_queues() -> Iterator[None]:
    yield
    reset_memory_queues()


@dataclasses.dataclass(eq=False)
class FakeResource(Resource):
    data: str


@pytest.fixture
def fake_resource_class() -> str:
    return __name__ + "." + FakeResource.__name__
