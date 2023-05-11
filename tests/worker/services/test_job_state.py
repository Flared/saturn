import typing as t

from unittest.mock import Mock

import pytest

from saturn_engine.utils import utcnow
from saturn_engine.worker.services.job_state.service import JobStateService
from saturn_engine.worker.services.manager import ServicesManager
from tests.conftest import FreezeTime
from tests.utils import HttpClientMock


class StateApiMock:
    PATH = "/api/jobs/_states"

    def __init__(self, client: HttpClientMock):
        self.client = client

    @property
    def get(self) -> Mock:
        return self.client.get(self.PATH)

    @property
    def put(self) -> Mock:
        return self.client.put(self.PATH)

    def reset_mock(self) -> None:
        self.client.reset_mock()


@pytest.fixture
def state_api_mock(
    http_client_mock: HttpClientMock,
) -> StateApiMock:
    return StateApiMock(http_client_mock)


@pytest.fixture
async def job_state_service(
    state_api_mock: StateApiMock,
    services_manager: ServicesManager,
    fake_http_client: t.Any,
) -> JobStateService:
    await services_manager._reload_service(fake_http_client(state_api_mock))
    return await services_manager._reload_service(JobStateService)


@pytest.mark.asyncio
async def test_job_state_update(
    state_api_mock: StateApiMock,
    frozen_time: FreezeTime,
    job_state_service: JobStateService,
) -> None:
    job_state_service.set_job_cursor(job_name="job-1", cursor="1")
    job_state_service.set_job_cursor(job_name="job-2", cursor="2")
    job_state_service.set_job_failed(
        job_name="job-2", error=ValueError("test"), failed_at=utcnow()
    )
    job_state_service.set_job_cursor(job_name="job-1", cursor="3")
    job_state_service.set_job_completed(job_name="job-3", completed_at=utcnow())

    job_state_service.set_job_item_cursor(
        job_name="job-3", message_cursor="1", cursor="a"
    )
    job_state_service.set_job_item_cursor(
        job_name="job-3", message_cursor="2", cursor="b"
    )
    job_state_service.set_job_item_cursor(
        job_name="job-3", message_cursor="3", cursor="c"
    )
    await job_state_service.flush()
    state_api_mock.put.assert_called_once_with(
        json={
            "jobs_state": {
                "job-1": {"cursor": "3"},
                "job-2": {
                    "cursor": "2",
                    "error": 'ValueError("test")',
                    "completed_at": "2018-01-02T00:00:03.300000+00:00",
                },
                "job-3": {
                    "completed_at": "2018-01-02T00:00:03.300000+00:00",
                    "items_cursor": {
                        "1": "a",
                        "2": "b",
                        "3": "c",
                    },
                },
            }
        }
    )

    state_api_mock.reset_mock()
    job_state_service.set_job_cursor(job_name="job-1", cursor="10")
    await job_state_service.flush()
    state_api_mock.put.assert_called_once_with(
        json={
            "jobs": {
                "job-1": {"cursor": "10"},
            }
        }
    )

    state_api_mock.reset_mock()
    await job_state_service.flush()
    state_api_mock.put.assert_not_called()
