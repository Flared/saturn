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
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace import sampling
from opentelemetry.sdk.trace.export import SimpleSpanProcessor

from saturn_engine.client.worker_manager import WorkerManagerClient
from saturn_engine.config import Config
from saturn_engine.core import Cursor
from saturn_engine.core import JobId
from saturn_engine.core import PipelineInfo
from saturn_engine.core import QueuePipeline
from saturn_engine.core import Resource
from saturn_engine.core import TopicMessage
from saturn_engine.core.api import ComponentDefinition
from saturn_engine.core.api import LockResponse
from saturn_engine.core.api import OutputDefinition
from saturn_engine.core.api import QueueItemWithState
from saturn_engine.worker.broker import Broker
from saturn_engine.worker.broker import WorkManagerInit
from saturn_engine.worker.executors import Executor
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.executors.executable import ExecutableQueue
from saturn_engine.worker.executors.parkers import Parkers
from saturn_engine.worker.executors.process import PoolType
from saturn_engine.worker.executors.process import ProcessExecutor
from saturn_engine.worker.executors.queue import ExecutorQueue
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.resources.provider import ResourcesProvider
from saturn_engine.worker.services import MinimalService
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.api_client import ApiClient
from saturn_engine.worker.services.job_state.service import CursorsStatesFetcher
from saturn_engine.worker.services.job_state.service import JobStateService
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.services.rabbitmq import RabbitMQService
from saturn_engine.worker.topics import MemoryTopic
from saturn_engine.worker.topics import Topic
from saturn_engine.worker.topics.memory import reset as reset_memory_queues
from saturn_engine.worker.work_manager import WorkManager
from tests.utils import HttpClientMock
from tests.utils import TimeForwardLoop
from tests.utils.metrics import MetricsCapture
from tests.utils.span_exporter import InMemorySpanExporter


@pytest.fixture
def worker_manager_client() -> Mock:
    _worker_manager_client = create_autospec(WorkerManagerClient, instance=True)
    _worker_manager_client.lock.return_value = LockResponse(
        items=[],
        resources=[],
        resources_providers=[],
        executors=[],
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


def mock_executor_maker(services: Services) -> Executor:
    return create_autospec(Executor, instance=True)


@pytest.fixture
async def executor_queue_maker(
    services_manager: ServicesManager,
) -> AsyncIterator[t.Callable[..., ExecutorQueue]]:
    async with contextlib.AsyncExitStack() as stack:

        def maker(
            executor: t.Optional[Executor] = None,
            services: Services = services_manager.services,
        ) -> ExecutorQueue:
            executor = executor or mock_executor_maker(services_manager.services)
            manager = ExecutorQueue(
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
def message_maker(fake_pipeline_info: PipelineInfo) -> t.Callable[..., PipelineMessage]:
    def maker(
        pipeline_info: PipelineInfo = fake_pipeline_info,
        args: t.Optional[dict[str, object]] = None,
        tags: t.Optional[dict[str, str]] = None,
        config: t.Optional[dict[str, dict[str, t.Any]]] = None,
    ) -> PipelineMessage:
        return PipelineMessage(
            info=pipeline_info or fake_pipeline_info,
            message=TopicMessage(args=args or {}, tags=tags or {}, config=config or {}),
        )

    return maker


@pytest.fixture
def fake_queue_item(
    fake_pipeline_info: PipelineInfo,
) -> QueueItemWithState:
    return QueueItemWithState(
        name=JobId("fake-queue"),
        pipeline=QueuePipeline(
            info=fake_pipeline_info,
            args={},
        ),
        labels={"owner": "team-saturn"},
        output={},
        input=ComponentDefinition(
            name="fake-topic",
            type="MemoryTopic",
        ),
    )


@pytest.fixture
def fake_topic() -> MemoryTopic:
    return MemoryTopic(MemoryTopic.Options(name="fake_topic"))


@pytest.fixture
def executable_queue_maker(
    fake_queue_item: QueueItemWithState,
    fake_topic: Topic,
    services_manager: ServicesManager,
) -> t.Callable[..., ExecutableQueue]:
    def maker(
        *,
        definition: QueueItemWithState = fake_queue_item,
        topic: Topic = fake_topic,
        output: t.Union[dict, None] = None,
    ) -> ExecutableQueue:
        return ExecutableQueue(
            definition=definition,
            topic=topic,
            output=output or {},
            services=services_manager.services,
        )

    return maker


@pytest.fixture
def fake_executable_queue(
    executable_queue_maker: t.Callable[..., ExecutableQueue],
) -> ExecutableQueue:
    return executable_queue_maker()


@pytest.fixture
def fake_executable_maker_with_output(
    fake_pipeline_info: PipelineInfo,
    fake_topic: Topic,
    services_manager: ServicesManager,
) -> t.Callable[..., ExecutableMessage]:
    def maker(
        *,
        message: t.Optional[TopicMessage] = None,
        parker: t.Optional[Parkers] = None,
        pipeline_info: PipelineInfo = fake_pipeline_info,
        output: t.Optional[dict[str, list[OutputDefinition]]] = None,
    ) -> ExecutableMessage:
        queue_item = QueueItemWithState(
            name=JobId("fake-failing-queue"),
            pipeline=QueuePipeline(
                info=fake_pipeline_info,
                args={},
            ),
            labels={"owner": "team-saturn"},
            output=output or {},
            input=ComponentDefinition(
                name="fake-topic",
                type="MemoryTopic",
            ),
        )

        output_topics: dict[str, list[Topic]] = {}
        for channel, topic_items in queue_item.output.items():
            output_topics[channel] = [
                MemoryTopic(MemoryTopic.Options(name=topic_item.name))
                for topic_item in topic_items
                if isinstance(topic_item, ComponentDefinition)
            ]

        executable_queue = ExecutableQueue(
            definition=queue_item,
            topic=fake_topic,
            output=output_topics,
            services=services_manager.services,
        )

        return ExecutableMessage(
            queue=executable_queue,
            message=PipelineMessage(
                info=pipeline_info or fake_pipeline_info,
                message=message or TopicMessage(args={}),
            ),
            parker=parker or Parkers(),
            output=executable_queue.output,
        )

    return maker


@pytest.fixture
def executable_maker(
    fake_pipeline_info: PipelineInfo,
    fake_executable_queue: ExecutableQueue,
) -> t.Callable[..., ExecutableMessage]:
    def maker(
        *,
        message: t.Optional[TopicMessage] = None,
        parker: t.Optional[Parkers] = None,
        pipeline_info: PipelineInfo = fake_pipeline_info,
        output: t.Optional[dict[str, list[Topic]]] = None,
        executable_queue: ExecutableQueue = fake_executable_queue,
    ) -> ExecutableMessage:
        return ExecutableMessage(
            queue=executable_queue,
            message=PipelineMessage(
                info=pipeline_info or fake_pipeline_info,
                message=message or TopicMessage(args={}),
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


class FakeResourcesProvider(ResourcesProvider["FakeResourcesProvider.Options"]):
    @dataclasses.dataclass
    class Options:
        pass

    async def open(self) -> None:
        pass


@pytest.fixture
def fake_resources_provider_class() -> str:
    return __name__ + "." + FakeResourcesProvider.__name__


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

        _rabbitmq_service = services_manager._load_service(RabbitMQService)
        await _rabbitmq_service.open()
        for connection in _rabbitmq_service.connections:
            await connection
        return _rabbitmq_service

    yield loader


@pytest.fixture
async def rabbitmq_service(
    rabbitmq_service_loader: t.Callable[..., Awaitable[RabbitMQService]]
) -> RabbitMQService:
    return await rabbitmq_service_loader()


@pytest.fixture(scope="session")
def _tracer() -> InMemorySpanExporter:
    provider = TracerProvider(sampler=sampling.ALWAYS_ON)
    exporter = InMemorySpanExporter()
    processor = SimpleSpanProcessor(exporter)
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    return exporter


@pytest.fixture
def span_exporter(_tracer: InMemorySpanExporter) -> InMemorySpanExporter:
    _tracer.clear()
    return _tracer


@pytest.fixture
def metrics_capture() -> Iterator[MetricsCapture]:
    helper = MetricsCapture()
    helper.setup_provider()
    yield helper
    helper.reset_provider()


@pytest.fixture
async def executor(services_manager: ServicesManager) -> AsyncIterator[Executor]:
    executor = ProcessExecutor(
        ProcessExecutor.Options(max_workers=1, pool_type=PoolType.THREAD),
        services=services_manager.services,
    )
    yield executor
    await executor.close()


@pytest.fixture
async def fake_http_client_service(
    http_client_mock: HttpClientMock,
    services_manager: ServicesManager,
) -> t.Any:
    class FakeHttpClient(MinimalService):
        name = "http_client"

        async def open(self) -> None:
            self.session = http_client_mock.client()

        async def close(self) -> None:
            pass

    await services_manager._reload_service(FakeHttpClient)
    await services_manager._reload_service(ApiClient)


class InMemoryCursorsFetcher(CursorsStatesFetcher):
    def __init__(self) -> None:
        self.states: dict[JobId, dict[Cursor, dict]] = {}

    async def fetch(
        self, job_name: JobId, *, cursors: list[Cursor]
    ) -> dict[Cursor, dict]:
        return {c: s for c, s in self.states[job_name].items() if c in cursors}

    def set_states(self, states: dict[JobId, dict[Cursor, dict]]) -> None:
        self.states = states


@pytest.fixture
async def inmemory_cursors_fetcher(
    services_manager: ServicesManager,
) -> InMemoryCursorsFetcher:
    fetcher = InMemoryCursorsFetcher()
    job_state_service = services_manager.services.cast_service(JobStateService)
    job_state_service._cursors_fetcher = fetcher
    return fetcher
