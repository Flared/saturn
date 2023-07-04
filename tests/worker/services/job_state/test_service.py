import typing as t

import pytest

from saturn_engine.core import Cursor
from saturn_engine.core import JobId
from saturn_engine.utils import utcnow
from saturn_engine.worker.services.job_state.service import JobStateService
from saturn_engine.worker.services.manager import ServicesManager
from tests.conftest import FreezeTime
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
        job_name=JobId("job-3"), cursor_state="1", cursor=Cursor("a")
    )
    job_state_service.set_job_cursor_state(
        job_name=JobId("job-3"), cursor_state="2", cursor=Cursor("b")
    )
    job_state_service.set_job_cursor_state(
        job_name=JobId("job-3"), cursor_state="3", cursor=Cursor("c")
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
                            "a": "1",
                            "b": "2",
                            "c": "3",
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
