import typing as t

import contextlib
import dataclasses
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Iterator
from unittest.mock import Mock
from unittest.mock import create_autospec

import aio_pika
import pytest

from saturn_engine.client.worker_manager import WorkerManagerClient
from saturn_engine.config import Config
from saturn_engine.core import PipelineInfo
from saturn_engine.core import Resource
from saturn_engine.core import TopicMessage
from saturn_engine.core.api import LockResponse
from saturn_engine.worker.broker import Broker
from saturn_engine.worker.broker import WorkManagerInit
from saturn_engine.worker.executors import Executor
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.executors.parkers import Parkers
from saturn_engine.worker.executors.queue import ExecutorQueue
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.resources_manager import ResourcesManager
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.services.rabbitmq import RabbitMQService
from saturn_engine.worker.topics import Topic
from saturn_engine.worker.topics.memory import reset as reset_memory_queues
from saturn_engine.worker.work_manager import WorkManager
from tests.utils import TimeForwardLoop


@pytest.fixture
def worker_manager_client() -> Mock:
    _worker_manager_client = create_autospec(WorkerManagerClient, instance=True)
    _worker_manager_client.lock.return_value = LockResponse(
        items=[], resources=[], executors=[]
    )
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
async def services_manager_maker(
    config: Config,
) -> AsyncIterator[t.Callable[[Config], Awaitable[ServicesManager]]]:
    services = []

    async def maker(config: Config = config) -> ServicesManager:
        services_manager = ServicesManager(config)
        await services_manager.open()
        services.append(services_manager)
        return services_manager

    yield maker
    for service in services:
        await service.close()


@pytest.fixture
async def services_manager(
    services_manager_maker: t.Callable[..., Awaitable[ServicesManager]]
) -> AsyncIterator[ServicesManager]:
    yield await services_manager_maker()


@pytest.fixture
def resources_manager() -> ResourcesManager:
    return ResourcesManager()


def mock_executor_maker(services: Services) -> Executor:
    return create_autospec(Executor, instance=True)


@pytest.fixture
async def executor_queue_maker(
    resources_manager: ResourcesManager,
    services_manager: ServicesManager,
) -> AsyncIterator[t.Callable[..., ExecutorQueue]]:
    async with contextlib.AsyncExitStack() as stack:

        def maker(
            executor: t.Optional[Executor] = None,
            services: Services = services_manager.services,
        ) -> ExecutorQueue:
            executor = executor or mock_executor_maker(services_manager.services)
            manager = ExecutorQueue(
                resources_manager=resources_manager,
                executor=executor,
                services=services,
            )
            manager.start()
            stack.push_async_callback(manager.close)
            return manager

        yield maker


@pytest.fixture
async def broker_maker(
    work_manager_maker: WorkManagerInit, config: Config
) -> AsyncIterator[t.Callable[..., Broker]]:
    brokers = []

    def maker(
        work_manager: WorkManagerInit = work_manager_maker, config: Config = config
    ) -> Broker:
        _broker = Broker(work_manager=work_manager, config=config)
        brokers.append(_broker)
        return _broker

    yield maker

    for _broker in brokers:
        await _broker.close()
        _broker.stop()


@pytest.fixture
async def broker(broker_maker: t.Callable[..., Broker]) -> AsyncIterator[Broker]:
    yield broker_maker()


@pytest.fixture
def executable_maker(
    fake_pipeline_info: PipelineInfo,
) -> t.Callable[..., ExecutableMessage]:
    def maker(
        args: t.Optional[dict[str, object]] = None,
        parker: t.Optional[Parkers] = None,
        pipeline_info: PipelineInfo = fake_pipeline_info,
        output: t.Optional[dict[str, list[Topic]]] = None,
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


@pytest.fixture
async def rabbitmq_url(event_loop: TimeForwardLoop, config: Config) -> str:
    event_loop.forward_time = False
    url = config.c.rabbitmq.url
    try:
        connection = await aio_pika.connect(url)
        await connection.close()
    except Exception:
        raise pytest.skip("No connection to RabbmitMQ server.") from None
    return url


@pytest.fixture
async def rabbitmq_service_loader(
    services_manager: ServicesManager, rabbitmq_url: str
) -> AsyncIterator[t.Callable[..., Awaitable[RabbitMQService]]]:
    async def loader(
        services_manager: ServicesManager = services_manager,
    ) -> RabbitMQService:
        if services_manager.has_loaded(RabbitMQService):
            return services_manager.services.cast_service(RabbitMQService)

        services_manager._load_service(RabbitMQService)
        _rabbitmq_service = services_manager.services.rabbitmq
        await _rabbitmq_service.connection
        return _rabbitmq_service

    yield loader


@pytest.fixture
async def rabbitmq_service(
    rabbitmq_service_loader: t.Callable[..., Awaitable[RabbitMQService]]
) -> RabbitMQService:
    return await rabbitmq_service_loader()
