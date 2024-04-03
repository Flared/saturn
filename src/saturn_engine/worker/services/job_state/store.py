import typing as t

import contextlib
import dataclasses
from collections import defaultdict

from saturn_engine.core import Cursor
from saturn_engine.core import JobId
from saturn_engine.core import api
from saturn_engine.utils import utcnow


@dataclasses.dataclass
class JobCompletion(api.JobCompletion):
    def merge(self, new: "JobCompletion") -> "JobCompletion":
        self.completed_at = new.completed_at
        self.error = new.error
        return self


@dataclasses.dataclass
class JobState:
    cursor: t.Optional[Cursor] = None
    cursors_states: dict[Cursor, dict] = dataclasses.field(default_factory=dict)
    completion: t.Optional[JobCompletion] = None

    def merge(self, new: "JobState") -> "JobState":
        if new.cursor:
            self.cursor = new.cursor
        self.cursors_states.update(new.cursors_states)
        if new.completion:
            completion = new.completion
            if self.completion:
                completion = self.completion.merge(new.completion)
            self.completion = completion
        return self


@dataclasses.dataclass
class JobsStates:
    jobs: dict[JobId, JobState] = dataclasses.field(
        default_factory=lambda: defaultdict(JobState)
    )

    def merge(self, new: "JobsStates") -> "JobsStates":
        for job, state in new.jobs.items():
            new_state = state
            if job in self.jobs:
                new_state = self.jobs[job].merge(state)
            self.jobs[job] = new_state
        return self

    @property
    def is_empty(self) -> bool:
        return not self.jobs

    def get_cursors_states(
        self, job_name: JobId, cursors: list[Cursor]
    ) -> dict[Cursor, dict]:
        if job_name in self.jobs:
            states = self.jobs[job_name].cursors_states
            return {c: states[c] for c in states.keys() & cursors}
        return {}


class JobsStatesSyncStore:
    def __init__(self) -> None:
        self._current_state = JobsStates()
        self._flushing_state: t.Optional[JobsStates] = None

    def set_job_cursor(self, job_name: JobId, cursor: Cursor) -> None:
        self._current_state.jobs[job_name].cursor = cursor

    def set_job_completed(self, job_name: JobId) -> None:
        self._current_state.jobs[job_name].completion = JobCompletion(
            completed_at=utcnow(),
        )

    def set_job_failed(self, job_name: JobId, error: str) -> None:
        self._current_state.jobs[job_name].completion = JobCompletion(
            completed_at=utcnow(),
            error=error,
        )

    def set_job_cursor_state(
        self,
        job_name: JobId,
        *,
        cursor: Cursor,
        cursor_state: dict,
    ) -> None:
        self._current_state.jobs[job_name].cursors_states[cursor] = cursor_state

    @contextlib.contextmanager
    def flush(self) -> t.Iterator[JobsStates]:
        """Allow to retrieve the jobs state in a safe-way for flushing.
        The yielded object won't be updated while inside the context.
        If an error happen inside the context, the state is restored and
        merged with any change that occured during the flush.
        """
        self._flushing_state = self._current_state
        self._current_state = JobsStates()

        try:
            yield self._flushing_state
        except BaseException:
            self._current_state = self._flushing_state.merge(self._current_state)
            self._flushing_state = None
            raise

    def job_state(self, job_name: JobId) -> JobState:
        return self._current_state.jobs[job_name]

    def get_local_cursors_states(
        self, job_name: JobId, *, cursors: list[Cursor]
    ) -> dict[Cursor, dict]:
        states = self._current_state.get_cursors_states(job_name, cursors=cursors)
        cursors = list(set(cursors) - states.keys())
        if cursors and self._flushing_state:
            states |= self._flushing_state.get_cursors_states(job_name, cursors=cursors)
        return states
