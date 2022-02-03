from typing import Optional

from datetime import datetime

from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.orm import joinedload

from saturn_engine.database import AnySession
from saturn_engine.database import AnySyncSession
from saturn_engine.models import Job
from saturn_engine.stores import queues_store
from saturn_engine.utils import utcnow


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
