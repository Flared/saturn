import unittest.mock
from typing import Iterator
from unittest.mock import PropertyMock

import pytest
from flask.testing import FlaskClient

from saturn_engine import database
from saturn_engine.models import Base
from saturn_engine.worker_manager import server as worker_manager_server
from saturn_engine.worker_manager.config import Configuration
from saturn_engine.worker_manager.config.declarative import StaticDefinitions


@pytest.fixture
def static_definitions() -> Iterator[StaticDefinitions]:
    new_definitions = StaticDefinitions()
    with unittest.mock.patch.object(
        Configuration,
        "static_definitions",
        new_callable=PropertyMock,
    ) as new_property:
        new_property.return_value = new_definitions
        yield new_definitions


@pytest.fixture
def client(
    static_definitions: StaticDefinitions,
) -> Iterator[FlaskClient]:
    app = worker_manager_server.get_app()
    Base.metadata.drop_all(bind=database.engine())
    Base.metadata.create_all(bind=database.engine())
    with app.test_client() as client:
        yield client
