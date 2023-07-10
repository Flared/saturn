import dataclasses
from collections import defaultdict

from saturn_engine.client.worker_manager import WorkerManagerClient
from saturn_engine.core import Cursor
from saturn_engine.core import JobId
from saturn_engine.core.api import FetchCursorsStatesInput
from saturn_engine.core.api import FetchCursorsStatesResponse
from saturn_engine.core.api import JobsStatesSyncInput
from saturn_engine.core.api import QueueItemWithState
from saturn_engine.utils.asyncutils import DelayedThrottle
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.services.hooks import ItemsBatch

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


class CursorsStatesFetcher:
    def __init__(self, *, client: WorkerManagerClient, fetch_delay: float = 0) -> None:
        self.client = client
        self.pending_queries: dict[JobId, set[Cursor]] = defaultdict(set)
        self._delayed_fetch = DelayedThrottle(self._do_fetch, delay=fetch_delay)

    async def _do_fetch(self) -> FetchCursorsStatesResponse:
        queries = self.pending_queries
        self.pending_queries = defaultdict(set)
        cursors = {k: list(v) for k, v in queries.items()}
        return await self.client.fetch_cursors_states(
            FetchCursorsStatesInput(cursors=cursors)
        )

    async def fetch(
        self, job_name: JobId, *, cursors: list[Cursor]
    ) -> dict[Cursor, dict]:
        self.pending_queries[job_name].update(cursors)
        result = await self._delayed_fetch()
        return result.cursors.get(job_name, {})


class CursorState:
    name = "CursorState"


class JobStateService(Service[Services, Options]):
    name = "job_state"

    Services = Services
    Options = Options

    _store: JobsStatesSyncStore
    _delayed_flush: DelayedThrottle

    async def open(self) -> None:
        self.logger = getLogger(__name__, self)
        self._store = JobsStatesSyncStore()
        self._cursors_fetcher = CursorsStatesFetcher(
            client=self.services.api_client.client
        )
        self._delayed_flush = DelayedThrottle(
            self.flush, delay=self.options.flush_delay
        )

        self.services.hooks.work_queue_built.register(self.on_work_queue_built)
        self.services.hooks.items_batched.register(self.on_items_batched)
        self.services.hooks.message_polled.register(self.on_message_polled)

    async def on_work_queue_built(self, queue_item: QueueItemWithState) -> None:
        # Ensure the job batching is enabled when we have cursors states enabled.
        # Otherwise on_items_batch won't emit.
        if queue_item.config.get("job_state", {}).get("cursors_states_enabled"):
            queue_item.config.setdefault("job", {})["batching_enabled"] = True

    async def on_items_batched(self, batch: ItemsBatch) -> None:
        cursors = [c for i in batch.items if (c := i.cursor)]
        cursors_states: dict = await self.fetch_cursors_states(
            batch.job.queue_item.name, cursors=cursors
        )
        for item in batch.items:
            metadata = item.metadata.setdefault("job_state", {})
            metadata["cursor_state"] = cursors_states.get(item.cursor)

    async def on_message_polled(self, xmsg: ExecutableMessage) -> None:
        metadata = xmsg.message.message.metadata.get("job_state", {})
        cursor_state = metadata.get("cursor_state")
        if cursor_state:
            xmsg.message.set_meta_arg(meta_type=CursorState, value=cursor_state)

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

    async def fetch_cursors_states(
        self, job_name: JobId, *, cursors: list[Cursor]
    ) -> dict[Cursor, dict]:
        return await self._cursors_fetcher.fetch(job_name, cursors=cursors)

    def _maybe_flush(self) -> None:
        if self.options.auto_flush:
            self._delayed_flush.call_nowait()

    async def flush(self) -> None:
        self.logger.debug("Flushing")
        with self._store.flush() as state:
            if not state.is_empty:
                await self.flush_state(state)

    async def flush_state(self, state: JobsStates) -> None:
        # Have to cast the job states to dict since defaultdict break dataclasses.
        state = dataclasses.replace(state, jobs=dict(state.jobs))
        await self.services.api_client.client.sync(JobsStatesSyncInput(state=state))

    async def close(self) -> None:
        self.logger.info("Closing")
        self._maybe_flush()
        await self._delayed_flush.flush()
