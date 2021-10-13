from typing import Union

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from saturn.database import AnyAsyncSession
from saturn.database import AnySession
from saturn.models import Job


def create_job(
    *,
    session: Union[AnySession],
    queue_id: int,
) -> Job:
    job = Job(queue_id=queue_id)
    session.add(job)
    return job


async def get_jobs(session: AnyAsyncSession) -> list[Job]:
    return (
        (await session.execute(select(Job).options(joinedload(Job.queue))))
        .scalars()
        .all()
    )
