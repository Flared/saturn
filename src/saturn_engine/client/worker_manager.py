from typing import Optional

import socket

import aiohttp

from saturn_engine.core.api import JobsStatesSyncInput
from saturn_engine.core.api import JobsStatesSyncResponse
from saturn_engine.core.api import LockInput
from saturn_engine.core.api import LockResponse
from saturn_engine.utils import urlcat
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import fromdict


class WorkerManagerClient:
    def __init__(
        self,
        http_client: aiohttp.ClientSession,
        base_url: str,
        worker_id: Optional[str] = None,
    ) -> None:
        self.worker_id: str = worker_id or socket.gethostname()
        self.http_client = http_client
        self.base_url = base_url

    async def lock(self) -> LockResponse:
        lock_url = urlcat(self.base_url, "api/lock")
        json = asdict(LockInput(worker_id=self.worker_id))
        async with self.http_client.post(lock_url, json=json) as response:
            return fromdict(await response.json(), LockResponse)

    async def sync(self, sync: JobsStatesSyncInput) -> JobsStatesSyncResponse:
        state_url = urlcat(self.base_url, "api/jobs/_states")
        json = asdict(sync)
        async with self.http_client.put(state_url, json=json) as response:
            return fromdict(await response.json(), JobsStatesSyncResponse)
