import typing as t

import socket
from functools import cached_property

from saturn_engine.core.api import LockInput
from saturn_engine.core.api import LockResponse
from saturn_engine.utils import urlcat
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import fromdict

from .. import BaseServices
from ..http_client import HttpClient
from . import WorkerManagerClient


class WorkerManagerApiClient(
    WorkerManagerClient[
        "WorkerManagerApiClient.Services", "WorkerManagerApiClient.Options"
    ]
):
    class Services(BaseServices):
        http_client: HttpClient

    class Options:
        base_url: str
        worker_id: t.Optional[str]

    async def open(self) -> None:
        pass

    async def close(self) -> None:
        pass

    @cached_property
    def worker_id(self) -> str:
        return self.options.worker_id or socket.gethostname()

    async def lock(self) -> LockResponse:
        lock_url = urlcat(self.options.base_url, "api/lock")
        json = asdict(LockInput(worker_id=self.worker_id))
        async with self.services.http_client.session.post(
            lock_url, json=json
        ) as response:
            return fromdict(await response.json(), LockResponse)
