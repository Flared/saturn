from typing import Iterator

import pytest
from flask.testing import FlaskClient

from saturn import database
from saturn.worker_manager import server as worker_manager_server


@pytest.fixture
def client() -> Iterator[FlaskClient]:
    app = worker_manager_server.get_app()
    database.drop_all()
    database.create_all()
    with app.test_client() as client:
        yield client
