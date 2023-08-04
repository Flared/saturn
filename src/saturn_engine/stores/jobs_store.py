import typing as t
from typing import Optional

import time
from datetime import datetime

from sqlalchemy import select
from sqlalchemy import union_all
from sqlalchemy import update
from sqlalchemy.orm import joinedload

from saturn_engine.core import Cursor
from saturn_engine.core import JobId
from saturn_engine.core.api import JobsStates
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import StartJobInput
from saturn_engine.models import Job
from saturn_engine.models.job import JobCursorState
from saturn_engine.models.queue import Queue
from saturn_engine.stores import queues_store
from saturn_engine.utils import utcnow
from saturn_engine.utils.sqlalchemy import AnySession
from saturn_engine.utils.sqlalchemy import AnySyncSession
from saturn_engine.utils.sqlalchemy import upsert
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions


def create_job(
    *,
    session: AnySession,
    name: str,
    queue_name: str,
    job_definition_name: Optional[str] = None,
    completed_at: Optional[datetime] = None,
    started_at: Optional[datetime] = None,
    error: Optional[str] = None,
) -> Job:
    job = Job(
        name=name,
        queue_name=queue_name,
        job_definition_name=job_definition_name,
        completed_at=completed_at,
        started_at=started_at,
        error=error,
    )
    session.add(job)
    return job


def get_jobs(*, session: AnySyncSession) -> list[Job]:
    return session.execute(select(Job).options(joinedload(Job.queue))).scalars().all()


def get_job(name: str, session: AnySyncSession) -> Optional[Job]:
    return session.get(Job, name)


def get_last_job(*, session: AnySyncSession, job_definition_name: str) -> Optional[Job]:
    return (
        session.execute(
            select(Job)
            .where(Job.job_definition_name == job_definition_name)
            .order_by(Job.started_at.desc())
            .options(joinedload(Job.queue))
        )
        .scalars()
        .first()
    )


def update_job(
    name: str,
    *,
    session: AnySyncSession,
    cursor: Optional[str] = None,
    completed_at: Optional[datetime] = None,
    error: Optional[str] = None,
) -> None:
    noop_stmt = stmt = update(Job).where(Job.name == name)
    if cursor:
        stmt = stmt.values(cursor=cursor)
    if completed_at:
        stmt = stmt.values(completed_at=completed_at)
    if error:
        stmt = stmt.values(error=error)

    if completed_at:
        job = get_job(session=session, name=name)
        if not job:
            raise Exception("Updating unknown job")
        queues_store.disable_queue(name=job.queue_name, session=session)

    if stmt is noop_stmt:
        return

    session.execute(stmt)


def set_failed(
    name: str,
    *,
    session: AnySyncSession,
    error: str,
    completed_at: Optional[datetime] = None,
) -> None:
    if completed_at is None:
        completed_at = utcnow()
    update_job(name, session=session, error=error, completed_at=completed_at)


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
    queues_values = []

    for job_id, job_state in state.jobs.items():
        job_values: dict[str, t.Any] = {}
        job_cursors: list[dict[str, t.Any]] = []

        if job_state.cursor:
            job_values["cursor"] = job_state.cursor
        if job_state.completion:
            job_values["completed_at"] = job_state.completion.completed_at
            if (error := job_state.completion.error) is not None:
                job_values["error"] = error
            queues_values.append(
                {
                    "name": job_id,
                    "enabled": False,
                }
            )
        for cursor, cursor_state in job_state.cursors_states.items():
            job_definition_name = job_definition_by_name.get(job_id)
            if job_definition_name:
                job_cursors.append(
                    {
                        "job_definition_name": job_definition_name,
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
                JobCursorState.job_definition_name,
                JobCursorState.cursor,
            ],
            set_={JobCursorState.state: cursors_stmt.excluded.state},
        )
        session.execute(cursors_stmt)

    if jobs_values:
        session.bulk_update_mappings(Job, jobs_values)
    if queues_values:
        session.bulk_update_mappings(Queue, queues_values)


CursorsStates = dict[JobId, dict[Cursor, t.Optional[dict]]]


def fetch_cursors_states(
    query: dict[JobId, list[Cursor]],
    session: AnySyncSession,
) -> CursorsStates:
    # Generate a query for each jobs so we can UNION them.
    fetch_stmts = []
    for job, cursors in query.items():
        fetch_stmts.append(
            select(Job.name, JobCursorState)
            .join(
                JobCursorState,
                Job.job_definition_name == JobCursorState.job_definition_name,
            )
            .where(
                Job.name == job,
                JobCursorState.cursor.in_(cursors),
            )
        )

    # Create a default state with every cursor default to None
    states: CursorsStates = {
        job: {c: None for c in cursors} for job, cursors in query.items()
    }

    # Fill the states with DB values.
    rows = session.execute(union_all(*fetch_stmts)).all()
    for row in rows:
        states[row.name][row.cursor] = row.state

    return states


def start(
    start_input: StartJobInput,
    static_definitions: StaticDefinitions,
    session: AnySyncSession,
    restart: bool,
) -> Job:
    queue_item: QueueItem | None = None
    job_definition_name: str | None = start_input.job_definition_name
    job: Job | None = None

    if start_input.name:
        queue_item = static_definitions.jobs.get(start_input.name)
        if not queue_item:
            job = get_job(name=start_input.name, session=session)
            if job:
                job_definition_name = job.job_definition_name

    if job_definition_name:
        job_definition = static_definitions.job_definitions.get(job_definition_name)
        if job_definition:
            queue_item = job_definition.template
            if not job:
                job = get_last_job(
                    session=session, job_definition_name=job_definition_name
                )

    if not queue_item:
        raise ValueError("Job not Found.")

    if job and not job.completed_at:
        if restart:
            update_job(
                session=session,
                name=job.name,
                completed_at=utcnow(),
                error="Cancelled",
            )
        else:
            raise ValueError("Job already started.")

    job_name = f"{queue_item.name}-{int(time.time())}"
    job_queue = queues_store.create_queue(session=session, name=job_name)
    new_job = create_job(
        session=session,
        name=job_name,
        queue_name=job_queue.name,
        job_definition_name=job_definition_name,
    )
    return new_job
