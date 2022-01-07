from typing import Callable
from typing import Iterator

import pytest
from flask.testing import FlaskClient

from saturn_engine import database
from saturn_engine.core import api
from saturn_engine.models import Base
from saturn_engine.worker_manager import server as worker_manager_server
from saturn_engine.worker_manager.config.declarative import StaticDefinitions


@pytest.fixture
def static_definitions() -> Iterator[StaticDefinitions]:
    new_definitions = StaticDefinitions()
    yield new_definitions


@pytest.fixture
def fake_job_definition(
    static_definitions: StaticDefinitions,
    job_definition_maker: Callable[..., api.JobDefinition],
) -> api.JobDefinition:
    job_definition = job_definition_maker()
    static_definitions.job_definitions["test"] = job_definition
    return job_definition


@pytest.fixture
def client(
    static_definitions: StaticDefinitions,
) -> Iterator[FlaskClient]:
    app = worker_manager_server.get_app(
        config={
            "TESTING": True,
        },
    )
    app.saturn.static_definitions = static_definitions
    with app.app_context():
        Base.metadata.drop_all(bind=database.engine())
        Base.metadata.create_all(bind=database.engine())
        with app.test_client() as client:
            yield client
