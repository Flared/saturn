from saturn_engine.core.api import SyncInput
from saturn_engine.core.api import SyncResponse
from saturn_engine.utils import urlcat
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import fromdict
from saturn_engine.core import Cursor
from saturn_engine.core import JobId
import abc
import typing as t
import dataclasses
from datetime import datetime

from saturn_engine.utils.asyncutils import DelayedThrottle
from saturn_engine.utils.options import SymbolName

from .. import BaseServices
from .. import Service
from ..api_client import ApiClient
from .store import JobsStatesSyncStore, JobsStates


class Services(BaseServices):
    http_client: HttpClient


class StateFlusher(abc.ABC):
    @abc.abstractmethod
    async def flush(self, state: t.Any) -> None:
        pass

@dataclasses.dataclass
class Options:
    flush_delay: float = 1.0


class JobStateService(Service[Services, Options]):
    name = "job_state"

    Services = Services
    Options = Options

    _store: JobsStatesSyncStore
    _delayed_flush: DelayedThrottle

    async def open(self) -> None:
        self._store = JobsStatesSyncStore()
        self._delayed_flush = DelayedThrottle(self.flush, delay=self.options.flush_delay)

    def set_job_cursor(self, job_name: JobId, cursor: Cursor) -> None:
        self._store.set_job_cursor(job_name, cursor)
        self._delayed_flush()

    def set_job_completed(self, job_name: JobId) -> None:
        self._store.set_job_completed(job_name)
        self._delayed_flush()

    def set_job_failed(
        self, job_name: JobId, *, error: Exception
    ) -> None:
        self._store.set_job_failed(job_name, str(error))
        self._delayed_flush()

    async def flush(self) -> None:
        with self._store.flush() as state:
            await self.flush_state(state)

    async def flush_state(self, state: JobsStates) -> None:
        await self.services.api_client.client.sync(SyncInput(state=state))

