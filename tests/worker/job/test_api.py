import asyncio

import pytest

from saturn_engine.worker.job.api import ApiJobStore
from tests.utils import HttpClientMock


@pytest.mark.asyncio
async def test_api_jobstore(http_client_mock: HttpClientMock) -> None:
    http_client_mock.get("/api/jobs/1").return_value = {
        "data": {"id": 1, "cursor": None}
    }

    job_store = ApiJobStore(
        http_client=http_client_mock.client(),
        base_url="",
        job_id=1,
    )

    assert await job_store.load_cursor() is None

    http_client_mock.get("/api/jobs/1").return_value = {
        "data": {"id": 1, "cursor": "10"}
    }
    assert await job_store.load_cursor() == "10"

    http_client_mock.put("/api/jobs/1").return_value = {}
    await job_store.save_cursor(after="20")
    http_client_mock.put("/api/jobs/1").assert_not_called()

    await asyncio.sleep(0.5)
    await job_store.save_cursor(after="30")
    http_client_mock.put("/api/jobs/1").assert_not_called()

    await asyncio.sleep(0.6)
    http_client_mock.put("/api/jobs/1").assert_called_once_with(json={"cursor": "30"})

    await asyncio.sleep(1.1)
    http_client_mock.put("/api/jobs/1").assert_called_once_with(json={"cursor": "30"})
