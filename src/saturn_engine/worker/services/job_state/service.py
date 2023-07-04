import dataclasses

from saturn_engine.core import Cursor
from saturn_engine.core import JobId
from saturn_engine.core.api import JobsStatesSyncInput
from saturn_engine.utils.asyncutils import DelayedThrottle

from .. import BaseServices
from .. import Service
from ..api_client import ApiClient
from .store import JobsStates
from .store import JobsStatesSyncStore


class Services(BaseServices):
    api_client: ApiClient


@dataclasses.dataclass
class Options:
    flush_delay: float = 10.0
    auto_flush: bool = True


class JobStateService(Service[Services, Options]):
    name = "job_state"

    Services = Services
    Options = Options

    _store: JobsStatesSyncStore
    _delayed_flush: DelayedThrottle

    async def open(self) -> None:
        self._store = JobsStatesSyncStore()
        self._delayed_flush = DelayedThrottle(
            self.flush, delay=self.options.flush_delay
        )

    def set_job_cursor(self, job_name: JobId, *, cursor: Cursor) -> None:
        self._store.set_job_cursor(job_name, cursor)
        self._maybe_flush()

    def set_job_completed(self, job_name: JobId) -> None:
        self._store.set_job_completed(job_name)
        self._maybe_flush()

    def set_job_failed(self, job_name: JobId, *, error: Exception) -> None:
        self._store.set_job_failed(job_name, f"{type(error).__name__}: {error}")
        self._maybe_flush()

    def set_job_cursor_state(
        self,
        job_name: JobId,
        *,
        cursor: Cursor,
        cursor_state: dict,
    ) -> None:
        self._store.set_job_cursor_state(
            job_name, cursor=cursor, cursor_state=cursor_state
        )
        self._maybe_flush()

    def _maybe_flush(self) -> None:
        if self.options.auto_flush:
            self._delayed_flush()

    async def flush(self) -> None:
        with self._store.flush() as state:
            if not state.is_empty:
                await self.flush_state(state)

    async def flush_state(self, state: JobsStates) -> None:
        # Have to cast the job states to dict since defaultdict break dataclasses.
        state = dataclasses.replace(state, jobs=dict(state.jobs))
        await self.services.api_client.client.sync(JobsStatesSyncInput(state=state))

    async def close(self) -> None:
        await self._delayed_flush.flush()
