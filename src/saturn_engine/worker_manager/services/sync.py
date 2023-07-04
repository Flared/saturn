import typing as t

import threading
import time
from datetime import datetime

from croniter import croniter
from sqlalchemy import select

from saturn_engine.core.api import JobsStates
from saturn_engine.models import Job
from saturn_engine.models.job import JobCursorState
from saturn_engine.stores import jobs_store
from saturn_engine.stores import queues_store
from saturn_engine.utils import utcnow
from saturn_engine.utils.sqlalchemy import AnySyncSession
from saturn_engine.utils.sqlalchemy import upsert
from saturn_engine.worker_manager.config.declarative import StaticDefinitions

_SYNC_LOCK = threading.Lock()


def sync_jobs(
    *,
    static_definitions: StaticDefinitions,
    session: AnySyncSession,
) -> None:
    if not _SYNC_LOCK.locked():
        with _SYNC_LOCK:
            _sync_jobs(static_definitions=static_definitions, session=session)


def _sync_jobs(
    *,
    static_definitions: StaticDefinitions,
    session: AnySyncSession,
) -> None:
    # Jobs with no interval
    for saturn_job in static_definitions.jobs.values():
        # Check if job exists and create if not
        existing_job = jobs_store.get_job(saturn_job.name, session)
        if not existing_job:
            job_queue = queues_store.create_queue(session=session, name=saturn_job.name)
            jobs_store.create_job(
                name=saturn_job.name,
                session=session,
                queue_name=job_queue.name,
            )

    # Jobs ran at an interval
    for job_definition in static_definitions.job_definitions.values():
        last_job = jobs_store.get_last_job(
            session=session,
            job_definition_name=job_definition.name,
        )

        if last_job:
            # If a job already exists, check it has completed and
            # the interval has elapsed to start a new one.
            if not last_job.completed_at:
                continue

            # If the last job completed with success, we check for
            # the job definition interval.
            if not last_job.error:
                scheduled_at = croniter(
                    job_definition.minimal_interval,
                    last_job.started_at,
                ).get_next(ret_type=datetime)
                if scheduled_at > utcnow():
                    continue

        job_name: str = f"{job_definition.name}-{int(time.time())}"
        queue = queues_store.create_queue(session=session, name=job_name)
        job = jobs_store.create_job(
            name=job_name,
            session=session,
            queue_name=queue.name,
            job_definition_name=job_definition.name,
        )

        # If the last job was an error, we resume from where we were.
        if last_job and last_job.error:
            job.cursor = last_job.cursor

        session.commit()


def sync_jobs_states(
    state: JobsStates,
    session: AnySyncSession,
) -> None:
    # Batch load all job definition name for cursors state.
    job_with_cursors_states = [
        job_id for job_id, job_state in state.jobs.items() if job_state.cursors_states
    ]
    jobs_definitions = session.execute(
        select(Job.name, Job.job_definition_name).where(
            Job.name.in_(job_with_cursors_states)
        )
    )
    job_definition_by_name = {j.name: j.job_definition_name for j in jobs_definitions}

    jobs_values = []
    jobs_cursors = []

    for job_id, job_state in state.jobs.items():
        job_values: dict[str, t.Any] = {}
        job_cursors: list[dict[str, t.Any]] = []

        if job_state.cursor:
            job_values["cursor"] = job_state.cursor
        if job_state.completion:
            job_values["completed_at"] = job_state.completion.completed_at
            if (error := job_state.completion.error) is not None:
                job_values["error"] = error
        for cursor, cursor_state in job_state.cursors_states.items():
            job_cursors.append(
                {
                    "job": job_definition_by_name.get(job_id) or job_id,
                    "cursor": cursor,
                    "state": cursor_state,
                }
            )

        if job_values:
            job_values["name"] = job_id
            jobs_values.append(job_values)
        if job_cursors:
            jobs_cursors.extend(job_cursors)

    if jobs_cursors:
        cursors_stmt = upsert(session)(JobCursorState).values(jobs_cursors)
        cursors_stmt = cursors_stmt.on_conflict_do_update(
            index_elements=[
                JobCursorState.job,
                JobCursorState.cursor,
            ],
            set_={JobCursorState.state: cursors_stmt.excluded.state},
        )
        session.execute(cursors_stmt)

    if jobs_values:
        session.bulk_update_mappings(Job, jobs_values)
