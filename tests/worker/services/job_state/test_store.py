from saturn_engine.core import Cursor
from saturn_engine.core import JobId
from saturn_engine.utils import utcnow
from saturn_engine.worker.services.job_state.store import Completion
from saturn_engine.worker.services.job_state.store import JobsStates
from saturn_engine.worker.services.job_state.store import JobsStatesSyncStore
from saturn_engine.worker.services.job_state.store import JobState


def test_flush_state(frozen_time: object) -> None:
    state = JobsStatesSyncStore()
    job_1_id = JobId("j1")
    job_2_id = JobId("j2")
    job_3_id = JobId("j3")
    state.set_job_cursor(job_1_id, Cursor("1"))
    state.set_job_cursor(job_2_id, Cursor("2"))

    try:
        with state.flush() as flush_state:
            state.set_job_completed(job_3_id)
            state.set_job_cursor(job_2_id, Cursor("3"))
            assert flush_state == JobsStates(
                jobs={
                    job_1_id: JobState(
                        cursor=Cursor("1"),
                    ),
                    job_2_id: JobState(
                        cursor=Cursor("2"),
                    ),
                }
            )
            raise ValueError()
    except ValueError:
        pass

    state.set_job_cursor(job_1_id, Cursor("2"))
    with state.flush() as flush_state:
        state.set_job_cursor(job_2_id, Cursor("4"))
        assert flush_state == JobsStates(
            jobs={
                job_1_id: JobState(
                    cursor=Cursor("2"),
                ),
                job_2_id: JobState(
                    cursor=Cursor("3"),
                ),
                job_3_id: JobState(completion=Completion(completed_at=utcnow())),
            }
        )

    state.set_job_cursor(job_2_id, Cursor("5"))
    with state.flush() as flush_state:
        assert flush_state == JobsStates(
            jobs={
                job_2_id: JobState(
                    cursor=Cursor("5"),
                ),
            }
        )

    with state.flush() as flush_state:
        assert flush_state.is_empty
