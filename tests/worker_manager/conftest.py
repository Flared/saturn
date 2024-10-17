import typing as t

import pytest
from flask.testing import FlaskClient
from sqlalchemy.orm import Session

from saturn_engine import database
from saturn_engine.core import JobId
from saturn_engine.core import api
from saturn_engine.models import Base
from saturn_engine.worker_manager import server as worker_manager_server
from saturn_engine.worker_manager.app import SaturnApp
from saturn_engine.worker_manager.config.declarative import StaticDefinitions


@pytest.fixture
def queue_pipeline_maker(
    fake_pipeline_info: api.PipelineInfo,
) -> t.Callable[..., api.QueuePipeline]:
    def maker() -> api.QueuePipeline:
        return api.QueuePipeline(
            info=fake_pipeline_info,
            args={},
        )

    return maker


@pytest.fixture
def topic_item_maker() -> t.Callable[..., api.ComponentDefinition]:
    def maker() -> api.ComponentDefinition:
        return api.ComponentDefinition(name="test", type="test", options={})

    return maker


@pytest.fixture
def queue_item_maker(
    queue_pipeline_maker: t.Callable[..., api.QueuePipeline],
    topic_item_maker: t.Callable[..., api.ComponentDefinition],
    fake_executor: api.ComponentDefinition,
) -> t.Callable[..., api.QueueItem]:
    def maker() -> api.QueueItem:
        return api.QueueItem(
            name=JobId("test"),
            pipeline=queue_pipeline_maker(),
            input=topic_item_maker(),
            output={"default": [topic_item_maker()]},
            labels={"owner": "team-saturn"},
            executor=fake_executor.name,
        )

    return maker


@pytest.fixture
def session() -> t.Iterator[Session]:
    yield database.session_factory()()


@pytest.fixture
def static_definitions() -> t.Iterator[StaticDefinitions]:
    new_definitions = StaticDefinitions()
    yield new_definitions


@pytest.fixture
def fake_executor(
    static_definitions: StaticDefinitions,
) -> api.ComponentDefinition:
    executor = api.ComponentDefinition(name="default", type="ProcessExecutor")
    static_definitions.executors[executor.name] = executor
    return executor


@pytest.fixture
def job_definition_maker(
    queue_item_maker: t.Callable[..., api.QueueItem]
) -> t.Callable[..., api.JobDefinition]:
    def maker() -> api.JobDefinition:
        return api.JobDefinition(
            name="test", template=queue_item_maker(), minimal_interval="@weekly"
        )

    return maker


@pytest.fixture
def fake_job_definition(
    static_definitions: StaticDefinitions,
    job_definition_maker: t.Callable[..., api.JobDefinition],
) -> api.JobDefinition:
    job_definition = job_definition_maker()
    static_definitions.job_definitions[job_definition.name] = job_definition
    return job_definition


@pytest.fixture
def app() -> t.Iterator[SaturnApp]:
    app = worker_manager_server.get_app(
        config={
            "TESTING": True,
        },
    )
    with app.app_context():
        Base.metadata.drop_all(bind=database.engine())
        Base.metadata.create_all(bind=database.engine())
        yield app


@pytest.fixture
def client(
    app: SaturnApp,
    static_definitions: StaticDefinitions,
) -> t.Iterator[FlaskClient]:
    app.saturn._static_definitions = static_definitions
    with app.test_client() as client:
        yield client
