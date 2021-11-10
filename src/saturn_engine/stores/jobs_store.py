from datetime import datetime
from typing import Optional
from typing import Union

from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.orm import joinedload

from saturn_engine.database import AnyAsyncSession
from saturn_engine.database import AnySession
from saturn_engine.models import Job


def create_job(
    *,
    session: Union[AnySession],
    queue_id: int,
) -> Job:
    job = Job(queue_id=queue_id)
    session.add(job)
    return job


async def get_jobs(*, session: AnyAsyncSession) -> list[Job]:
    return (
        (await session.execute(select(Job).options(joinedload(Job.queue))))
        .scalars()
        .all()
    )


async def get_job(job_id: int, session: AnyAsyncSession) -> Optional[Job]:
    return await session.get(Job, job_id)


async def update_job(
    job_id: int,
    *,
    cursor: Optional[str],
    completed_at: Optional[datetime],
    session: AnyAsyncSession,
) -> None:
    noop_stmt = stmt = update(Job).where(Job.id == job_id)
    if cursor:
        stmt = stmt.values(cursor=cursor)
    if completed_at:
        stmt = stmt.values(completed_at=completed_at)

    if stmt is noop_stmt:
        return

    await session.execute(stmt)
