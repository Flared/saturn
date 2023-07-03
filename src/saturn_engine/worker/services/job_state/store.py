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
    def merge(self, other: "JobCompletion") -> "JobCompletion":
        self.completed_at = other.completed_at
        self.error = other.error
        return self


@dataclasses.dataclass
class JobState(api.JobState):
    completion: t.Optional[JobCompletion] = None

    def merge(self, other: "JobState") -> "JobState":
        if other.cursor:
            self.cursor = other.cursor
        self.cursors_states.update(other.cursors_states)
        if other.completion:
            completion = other.completion
            if self.completion:
                completion = self.completion.merge(other.completion)
            self.completion = completion
        return self


@dataclasses.dataclass
class JobsStates(api.JobsStates):
    jobs: dict[JobId, JobState] = dataclasses.field(
        default_factory=lambda: defaultdict(JobState)
    )

    def merge(self, other: "JobsStates") -> "JobsStates":
        for job, state in other.jobs.items():
            new_state = state
            if job in self.jobs:
                new_state = self.jobs[job].merge(state)
            self.jobs[job] = new_state
        return self

    @property
    def is_empty(self) -> bool:
        return not self.jobs


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
        cursor_state: str,
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
            raise
        finally:
            self._flushing_state = None
