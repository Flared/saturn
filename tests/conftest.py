import asyncio
from collections.abc import Iterator
from typing import Union

import freezegun
import pytest
from freezegun.api import FrozenDateTimeFactory
from freezegun.api import StepTickTimeFactory
from sqlalchemy.orm import Session

from saturn_engine import database

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
