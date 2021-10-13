from typing import Iterator

import pytest
from flask.testing import FlaskClient

from saturn_engine import database
from saturn_engine.models import Base
from saturn_engine.worker_manager import server as worker_manager_server


@pytest.fixture
def client() -> Iterator[FlaskClient]:
    app = worker_manager_server.get_app()
    Base.metadata.drop_all(bind=database.engine())
    Base.metadata.create_all(bind=database.engine())
    with app.test_client() as client:
        yield client
