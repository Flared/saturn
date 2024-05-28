import typing as t

import dataclasses
import enum
import hashlib
from collections import defaultdict

from saturn_engine.client.worker_manager import WorkerManagerClient
from saturn_engine.core import Cursor
from saturn_engine.core import JobId
from saturn_engine.core import api
from saturn_engine.core.api import FetchCursorsStatesInput
from saturn_engine.core.api import JobsStatesSyncInput
from saturn_engine.core.api import QueueItemWithState
from saturn_engine.core.job_state import CursorState
from saturn_engine.core.job_state import CursorStateUpdated
from saturn_engine.core.types import CursorStateKey
from saturn_engine.utils import assert_never
from saturn_engine.utils.asyncutils import DelayedThrottle
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.services.hooks import ItemsBatch
from saturn_engine.worker.services.hooks import PipelineEventsEmitted

from .. import BaseServices
from .. import Service
from ..api_client import ApiClient
from .store import JobsStates
from .store import JobsStatesSyncStore


class Services(BaseServices):
    api_client: ApiClient


class CursorFormat(enum.Enum):
    RAW = "raw"
    SHA256 = "sha256"

    def format(self, cursor: Cursor) -> CursorStateKey:
        key: str
        if self is CursorFormat.RAW:
            key = cursor
        elif self is CursorFormat.SHA256:
            key = "sha256:" + hashlib.sha256(cursor.encode()).hexdigest()
        else:
            assert_never(self)
        return CursorStateKey(key)


@dataclasses.dataclass
class Options:
    flush_delay: float = 10.0
    auto_flush: bool = True
    fetch_cursor_formats: list[CursorFormat] = dataclasses.field(
        default_factory=lambda: [CursorFormat.SHA256]
    )
    save_cursor_format: CursorFormat = CursorFormat.SHA256


@dataclasses.dataclass
class FormattedRequest:
    by_cursor: dict[Cursor, list[CursorStateKey]]
    by_formatted: dict[CursorStateKey, Cursor]

    @classmethod
    def from_cursors(
        cls, cursors: t.Iterable[Cursor], formats: list[CursorFormat]
    ) -> "FormattedRequest":
        formatted_cursors = {}
        inverted_cursors = {}
        for cursor in cursors:
            formatted = [f.format(cursor) for f in formats]
            formatted_cursors[cursor] = formatted
            inverted_cursors.update({k: cursor for k in formatted})
        return cls(
            by_cursor=formatted_cursors,
            by_formatted=inverted_cursors,
        )

    def all_cursors(self) -> list[CursorStateKey]:
        return list(self.by_formatted.keys())

    def map(self, key: CursorStateKey) -> Cursor | None:
        return self.by_formatted.get(key)


class CursorsStatesFetcher:
    def __init__(
        self,
        *,
        client: WorkerManagerClient,
        formats: list[CursorFormat],
        fetch_delay: float = 0,
    ) -> None:
        self.client = client
        self.formats = formats
        self.pending_queries: dict[JobId, set[Cursor]] = defaultdict(set)
        self._delayed_fetch = DelayedThrottle(self._do_fetch, delay=fetch_delay)

    async def _do_fetch(self) -> dict[JobId, dict[Cursor, dict]]:
        queries = self.pending_queries
        self.pending_queries = defaultdict(set)

        # Convert Cursor into CursorStateKey
        formatted_cursors = {
            k: FormattedRequest.from_cursors(v, formats=self.formats)
            for k, v in queries.items()
        }
        cursors_keys = {k: v.all_cursors() for k, v in formatted_cursors.items()}
        response = await self.client.fetch_cursors_states(
            FetchCursorsStatesInput(cursors=cursors_keys)
        )

        # Retrieve original cursor from the CursorStateKey
        mapped_cursors = {}
        for job_name, cursor_states in response.cursors.items():
            mapped_cursors[job_name] = {
                fk: v
                for k, v in cursor_states.items()
                if v is not None
                and (fk := formatted_cursors[job_name].map(k)) is not None
            }
        return mapped_cursors

    async def fetch(
        self, job_name: JobId, *, cursors: list[Cursor]
    ) -> dict[Cursor, dict]:
        self.pending_queries[job_name].update(cursors)
        result = await self._delayed_fetch()
        return result.get(job_name, {})


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
            client=self.services.api_client.client,
            formats=self.options.fetch_cursor_formats,
        )
        self._delayed_flush = DelayedThrottle(
            self.flush, delay=self.options.flush_delay
        )

        self.services.hooks.work_queue_built.register(self.on_work_queue_built)
        self.services.hooks.items_batched.register(self.on_items_batched)
        self.services.hooks.message_polled.register(self.on_message_polled)
        self.services.hooks.pipeline_events_emitted.register(
            self.on_pipeline_events_emitted
        )

    async def on_work_queue_built(self, queue_item: QueueItemWithState) -> None:
        # Ensure the job batching is enabled when we have cursors states enabled.
        # Otherwise on_items_batch won't emit.
        if queue_item.config.get("job_state", {}).get("cursors_states_enabled"):
            queue_item.config.setdefault("job", {})["batching_enabled"] = True

    async def on_items_batched(self, batch: ItemsBatch) -> None:
        # Default `i.metadata["job_state"]["state_cursor"]` to `i.cursor`
        for item in batch.items:
            item.metadata.setdefault("job_state", {}).setdefault(
                "state_cursor", item.metadata.get("job", {}).get("cursor")
            )

        cursors = [
            c for i in batch.items if (c := i.metadata["job_state"]["state_cursor"])
        ]
        namespace = (
            batch.job.config.get("job_state", {}).get("cursors_states_namespace")
            or batch.job.name
        )

        cursors_states: dict = await self.fetch_cursors_states(
            namespace, cursors=cursors
        )
        for item in batch.items:
            metadata = item.metadata.setdefault("job_state", {})
            metadata["cursor_state"] = cursors_states.get(metadata["state_cursor"])

    async def on_message_polled(self, xmsg: ExecutableMessage) -> None:
        metadata = xmsg.message.message.metadata.get("job_state", {})
        cursor_state = metadata.get("cursor_state")
        if cursor_state:
            xmsg.message.set_meta_arg(meta_type=CursorState, value=cursor_state)

    async def on_pipeline_events_emitted(self, pevents: PipelineEventsEmitted) -> None:
        for event in pevents.events:
            if not isinstance(event, CursorStateUpdated):
                continue

            message = pevents.xmsg.message.message
            cursor = event.cursor or message.metadata.get("job_state", {}).get(
                "state_cursor"
            )
            if not cursor:
                continue

            queue = pevents.xmsg.queue.definition
            namespace = (
                queue.config.get("job_state", {}).get("cursors_states_namespace")
                or queue.name
            )
            self.set_job_cursor_state(
                namespace, cursor=cursor, cursor_state=event.state
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

    async def fetch_cursors_states(
        self, job_name: JobId, *, cursors: list[Cursor]
    ) -> dict[Cursor, dict]:
        # First check in local write cache before loading remote values.
        states = self._store.get_local_cursors_states(job_name, cursors=cursors)
        cursors = list(set(cursors) - states.keys())
        if cursors:
            states |= await self._cursors_fetcher.fetch(job_name, cursors=cursors)
        return states

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
        api_state = self._prepare_jobs_states(state)
        await self.services.api_client.client.sync(JobsStatesSyncInput(state=api_state))

    async def close(self) -> None:
        self.logger.info("Closing")
        self._maybe_flush()
        await self._delayed_flush.flush()

    def _prepare_jobs_states(self, states: JobsStates) -> api.JobsStates:
        jobs = {}
        for job, state in states.jobs.items():
            formatted_cursors_states = {
                self.options.save_cursor_format.format(k): v
                for k, v in state.cursors_states.items()
            }
            jobs[job] = api.JobState(
                cursor=state.cursor,
                cursors_states=formatted_cursors_states,
                completion=state.completion,
            )
        return api.JobsStates(jobs=jobs)
