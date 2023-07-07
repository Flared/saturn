import typing as t

import asyncio

import pytest

from saturn_engine.core import Cursor
from saturn_engine.core import JobId
from saturn_engine.utils import utcnow
from saturn_engine.worker.services.job_state.service import JobStateService
from saturn_engine.worker.services.manager import ServicesManager
from tests.conftest import FreezeTime
from tests.utils import EqualAnyOrder
from tests.utils import HttpClientMock


@pytest.fixture
async def job_state_service(
    services_manager: ServicesManager,
    fake_http_client_service: t.Any,
) -> JobStateService:
    return await services_manager._reload_service(JobStateService)


@pytest.mark.asyncio
async def test_job_state_update(
    http_client_mock: HttpClientMock,
    frozen_time: FreezeTime,
    job_state_service: JobStateService,
) -> None:
    http_client_mock.put("http://127.0.0.1:5000/api/jobs/_states").return_value = {}
    job_state_service.set_job_cursor(job_name=JobId("job-1"), cursor=Cursor("1"))
    job_state_service.set_job_cursor(job_name=JobId("job-2"), cursor=Cursor("2"))
    job_state_service.set_job_failed(job_name=JobId("job-2"), error=ValueError("test"))
    job_state_service.set_job_cursor(job_name=JobId("job-1"), cursor=Cursor("3"))
    job_state_service.set_job_completed(job_name=JobId("job-3"))

    job_state_service.set_job_cursor_state(
        job_name=JobId("job-3"), cursor_state={"x": "1"}, cursor=Cursor("a")
    )
    job_state_service.set_job_cursor_state(
        job_name=JobId("job-3"), cursor_state={"x": "2"}, cursor=Cursor("b")
    )
    job_state_service.set_job_cursor_state(
        job_name=JobId("job-3"), cursor_state={"x": "3"}, cursor=Cursor("c")
    )
    await job_state_service.flush()
    http_client_mock.put(
        "http://127.0.0.1:5000/api/jobs/_states"
    ).assert_called_once_with(
        json={
            "state": {
                "jobs": {
                    "job-1": {"completion": None, "cursors_states": {}, "cursor": "3"},
                    "job-2": {
                        "completion": {
                            "error": "ValueError: test",
                            "completed_at": utcnow(),
                        },
                        "cursors_states": {},
                        "cursor": "2",
                    },
                    "job-3": {
                        "completion": {
                            "error": None,
                            "completed_at": utcnow(),
                        },
                        "cursors_states": {
                            "a": {"x": "1"},
                            "b": {"x": "2"},
                            "c": {"x": "3"},
                        },
                        "cursor": None,
                    },
                }
            }
        }
    )

    http_client_mock.reset_mock()
    job_state_service.set_job_cursor(job_name=JobId("job-1"), cursor=Cursor("10"))
    await job_state_service.flush()
    http_client_mock.put(
        "http://127.0.0.1:5000/api/jobs/_states"
    ).assert_called_once_with(
        json={
            "state": {
                "jobs": {
                    "job-1": {
                        "completion": None,
                        "cursor": "10",
                        "cursors_states": {},
                    },
                }
            }
        }
    )

    http_client_mock.reset_mock()
    await job_state_service.flush()
    http_client_mock.put("http://127.0.0.1:5000/api/jobs/_states").assert_not_called()


async def test_job_state_fetch_cursors(
    http_client_mock: HttpClientMock,
    job_state_service: JobStateService,
) -> None:
    http_client_mock.post(
        "http://127.0.0.1:5000/api/jobs/_states/fetch"
    ).return_value = {
        "cursors": {
            "job-1": {
                "a": {"x": 1},
                "b": {"x": 2},
            },
            "job-2": {
                "c": None,
            },
        }
    }

    fetch_1 = job_state_service.fetch_cursors_states(
        JobId("job-1"), cursors=[Cursor("a"), Cursor("b")]
    )
    fetch_2 = job_state_service.fetch_cursors_states(
        JobId("job-2"), cursors=[Cursor("c")]
    )
    states_1, states_2 = await asyncio.gather(fetch_1, fetch_2)

    assert states_1 == {"a": {"x": 1}, "b": {"x": 2}}
    assert states_2 == {"c": None}

    http_client_mock.post(
        "http://127.0.0.1:5000/api/jobs/_states/fetch"
    ).assert_called_once_with(
        json={
            "cursors": {
                "job-1": EqualAnyOrder(["a", "b"]),
                "job-2": ["c"],
            }
        }
    )
