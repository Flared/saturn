from typing import Optional

import abc
import socket

import aiohttp

from saturn_engine.core.api import FetchCursorsStatesInput
from saturn_engine.core.api import FetchCursorsStatesResponse
from saturn_engine.core.api import JobsStatesSyncInput
from saturn_engine.core.api import JobsStatesSyncResponse
from saturn_engine.core.api import LockInput
from saturn_engine.core.api import LockResponse
from saturn_engine.utils import urlcat
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import fromdict


class AbstractWorkerManagerClient(abc.ABC):
    @abc.abstractmethod
    async def lock(self) -> LockResponse:
        pass

    @abc.abstractmethod
    async def sync(self, sync: JobsStatesSyncInput) -> JobsStatesSyncResponse:
        pass

    @abc.abstractmethod
    async def fetch_cursors_states(
        self, cursors: FetchCursorsStatesInput
    ) -> FetchCursorsStatesResponse:
        pass


class WorkerManagerClient(AbstractWorkerManagerClient):
    def __init__(
        self,
        http_client: aiohttp.ClientSession,
        base_url: str,
        worker_id: Optional[str] = None,
        selector: Optional[str] = None,
        executors: list[str] | None = None,
    ) -> None:
        self.worker_id: str = worker_id or socket.gethostname()
        self.selector: str | None = selector
        self.executors: list[str] | None = executors
        self.http_client = http_client
        self.base_url = base_url

    async def lock(self) -> LockResponse:
        lock_url = urlcat(self.base_url, "api/lock")
        json = asdict(
            LockInput(
                worker_id=self.worker_id,
                selector=self.selector,
                executors=self.executors,
            )
        )
        async with self.http_client.post(lock_url, json=json) as response:
            return fromdict(await response.json(), LockResponse)

    async def sync(self, sync: JobsStatesSyncInput) -> JobsStatesSyncResponse:
        state_url = urlcat(self.base_url, "api/jobs/_states")
        json = asdict(sync)
        async with self.http_client.put(state_url, json=json) as response:
            return fromdict(await response.json(), JobsStatesSyncResponse)

    async def fetch_cursors_states(
        self, cursors: FetchCursorsStatesInput
    ) -> FetchCursorsStatesResponse:
        state_url = urlcat(self.base_url, "api/jobs/_states/fetch")
        json = asdict(cursors)
        async with self.http_client.post(state_url, json=json) as response:
            return fromdict(await response.json(), FetchCursorsStatesResponse)
