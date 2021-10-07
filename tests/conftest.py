from typing import Iterator

import pytest
from sqlalchemy.orm import Session

from saturn import database


@pytest.fixture
def session() -> Iterator[Session]:
    yield database.session_factory()()
