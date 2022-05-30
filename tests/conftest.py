import typing as t

import asyncio
from collections.abc import Awaitable
from collections.abc import Iterator

import freezegun
import pytest
from freezegun.api import FrozenDateTimeFactory
from freezegun.api import StepTickTimeFactory
from freezegun.api import freeze_factories  # type: ignore[attr-defined]
from pytest_mock import MockerFixture

from saturn_engine.config import Config
from saturn_engine.config import default_config
from saturn_engine.core import api

from .config import config as test_config
from .utils import HttpClientMock
from .utils import TimeForwardLoop
from .utils.tcp_proxy import TcpProxy


@pytest.fixture
def http_client_mock(event_loop: TimeForwardLoop) -> HttpClientMock:
    return HttpClientMock(loop=event_loop)


FreezeTime = t.Union[FrozenDateTimeFactory, StepTickTimeFactory]


@pytest.fixture
def freezer() -> t.Any:
    return freezegun.freeze_time(
        "2018-01-02T00:00:00+00:00",
        ignore=["_pytest.runner"],
    )


@pytest.fixture
def frozen_time(freezer: t.Any) -> Iterator[FreezeTime]:
    frozen_time = freezer.start()
    yield frozen_time
    if len(freeze_factories):
        freezer.stop()


@pytest.fixture
def event_loop(
    frozen_time: FrozenDateTimeFactory,
    freezer: t.Any,
) -> Iterator[TimeForwardLoop]:
    """Define a custom event loop.
    This event loop use a custom Selector that wraps sleep forward.
    """
    loop = TimeForwardLoop(
        freezer=freezer,
        frozen_time=frozen_time,
    )
    yield loop
    loop.run_until_complete(loop.shutdown_asyncgens())
    loop.run_until_complete(loop.shutdown_default_executor())
    tasks = asyncio.all_tasks(loop)

    try:
        assert not tasks
    finally:
        loop.close()


def pipeline() -> None:
    ...


@pytest.fixture
def fake_pipeline(mocker: MockerFixture) -> Iterator[t.Callable]:
    mock = mocker.patch(__name__ + ".pipeline", autospec=True)
    yield mock


@pytest.fixture
def fake_pipeline_info(fake_pipeline: t.Callable) -> api.PipelineInfo:
    return api.PipelineInfo.from_pipeline(fake_pipeline)


@pytest.fixture
def queue_pipeline_maker(
    fake_pipeline_info: api.PipelineInfo,
) -> t.Callable[..., api.QueuePipeline]:
    def maker() -> api.QueuePipeline:
        return api.QueuePipeline(info=fake_pipeline_info, args={})

    return maker


@pytest.fixture
def topic_item_maker() -> t.Callable[..., api.TopicItem]:
    def maker() -> api.TopicItem:
        return api.TopicItem(name="test", type="test", options={})

    return maker


@pytest.fixture
def queue_item_maker(
    queue_pipeline_maker: t.Callable[..., api.QueuePipeline],
    topic_item_maker: t.Callable[..., api.TopicItem],
) -> t.Callable[..., api.QueueItem]:
    def maker() -> api.QueueItem:
        return api.QueueItem(
            name="test",
            pipeline=queue_pipeline_maker(),
            input=topic_item_maker(),
            output={"default": [topic_item_maker()]},
        )

    return maker


@pytest.fixture
def job_definition_maker(
    queue_item_maker: t.Callable[..., api.QueueItem]
) -> t.Callable[..., api.JobDefinition]:
    def maker() -> api.JobDefinition:
        return api.JobDefinition(
            name="test", template=queue_item_maker(), minimal_interval="@weekly"
        )

    return maker


@pytest.fixture(scope="session")
def config() -> Config:
    return Config().load_objects([default_config, test_config])


@pytest.fixture(scope="session")
def ray_cluster() -> None:
    try:
        import ray
    except ImportError:
        raise pytest.skip("Skipping ray tests") from None

    ray.init(local_mode=True)


@pytest.fixture(scope="session")
def remote_ray_cluster() -> None:
    try:
        import ray
    except ImportError:
        raise pytest.skip("Skipping ray tests") from None

    try:
        ray.init(address="auto", local_mode=False)
    except Exception:
        raise pytest.skip("Skipping remote ray tests") from None


@pytest.fixture
async def tcp_proxy() -> t.AsyncIterator[t.Callable[[int, int], Awaitable[TcpProxy]]]:
    proxies: list[TcpProxy] = []

    async def factory(src_port: int, dst_port: int) -> TcpProxy:
        proxy = TcpProxy(
            dst_port=dst_port,
            src_port=src_port,
        )
        await proxy.start()
        proxies.append(proxy)
        return proxy

    yield factory
    for proxy in proxies:
        await proxy.disconnect()
