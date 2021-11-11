import asyncio

import pytest

from saturn_engine.worker.job.api import ApiJobStore
from tests.conftest import FreezeTime
from tests.utils import HttpClientMock


@pytest.mark.asyncio
async def test_api_jobstore(
    http_client_mock: HttpClientMock, frozen_time: FreezeTime
) -> None:
    http_client_mock.get("/api/jobs/test").return_value = {
        "data": {"name": "test", "cursor": None}
    }

    job_store = ApiJobStore(
        http_client=http_client_mock.client(),
        base_url="",
        job_name="test",
    )

    assert await job_store.load_cursor() is None

    http_client_mock.get("/api/jobs/test").return_value = {
        "data": {"name": "test", "cursor": "10"}
    }
    assert await job_store.load_cursor() == "10"

    http_client_mock.put("/api/jobs/test").return_value = {}
    await job_store.save_cursor(after="20")
    http_client_mock.put("/api/jobs/test").assert_not_called()

    await asyncio.sleep(0.5)
    await job_store.save_cursor(after="30")
    http_client_mock.put("/api/jobs/test").assert_not_called()

    await asyncio.sleep(0.6)
    http_client_mock.put("/api/jobs/test").assert_called_once_with(
        json={"cursor": "30", "completed_at": None}
    )

    await asyncio.sleep(1.1)
    http_client_mock.put("/api/jobs/test").assert_called_once_with(
        json={"cursor": "30", "completed_at": None}
    )

    http_client_mock.reset_mock()
    await job_store.save_cursor(after="40")
    await job_store.set_completed()
    http_client_mock.put("/api/jobs/test").assert_called_once_with(
        json={"cursor": "40", "completed_at": "2018-01-02T00:00:00+00:00"}
    )
    http_client_mock.reset_mock()

    await asyncio.sleep(1.1)
    http_client_mock.put("/api/jobs/test").assert_not_called()
