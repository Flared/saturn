import asyncio
from collections.abc import Iterator
from typing import Callable
from typing import Union

import freezegun
import pytest
from freezegun.api import FrozenDateTimeFactory
from freezegun.api import StepTickTimeFactory
from pytest_mock import MockerFixture
from sqlalchemy.orm import Session

from saturn_engine import database
from saturn_engine.core import api

from .utils import HttpClientMock
from .utils import TimeForwardLoop


@pytest.fixture
def http_client_mock(event_loop: TimeForwardLoop) -> HttpClientMock:
    return HttpClientMock(loop=event_loop)


@pytest.fixture
def session() -> Iterator[Session]:
    yield database.session_factory()()


@pytest.fixture
def event_loop() -> Iterator[TimeForwardLoop]:
    """Define a custom event loop.
    This event loop use a custom Selector that wraps sleep forward.
    """
    loop = TimeForwardLoop()
    yield loop
    tasks = asyncio.all_tasks(loop)
    try:
        assert not tasks
    finally:
        loop.close()


FreezeTime = Union[FrozenDateTimeFactory, StepTickTimeFactory]


@pytest.fixture
def frozen_time() -> Iterator[FreezeTime]:
    with freezegun.freeze_time(
        "2018-01-02T00:00:00+00:00",
        ignore=["_pytest.runner"],
    ) as frozen_time:
        yield frozen_time


def pipeline() -> None:
    ...


@pytest.fixture
def fake_pipeline(mocker: MockerFixture) -> Iterator[Callable]:
    mock = mocker.patch(__name__ + ".pipeline", autospec=True)
    yield mock


@pytest.fixture
def fake_pipeline_info(fake_pipeline: Callable) -> api.PipelineInfo:
    return api.PipelineInfo.from_pipeline(fake_pipeline)


@pytest.fixture
def queue_pipeline_maker(
    fake_pipeline_info: api.PipelineInfo,
) -> Callable[..., api.QueuePipeline]:
    def maker() -> api.QueuePipeline:
        return api.QueuePipeline(info=fake_pipeline_info, args={})

    return maker


@pytest.fixture
def topic_item_maker() -> Callable[..., api.TopicItem]:
    def maker() -> api.TopicItem:
        return api.TopicItem(name="test", type="test", options={})

    return maker


@pytest.fixture
def queue_item_maker(
    queue_pipeline_maker: Callable[..., api.QueuePipeline],
    topic_item_maker: Callable[..., api.TopicItem],
) -> Callable[..., api.QueueItem]:
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
    queue_item_maker: Callable[..., api.QueueItem]
) -> Callable[..., api.JobDefinition]:
    def maker() -> api.JobDefinition:
        return api.JobDefinition(
            name="test", template=queue_item_maker(), minimal_interval="@weekly"
        )

    return maker
