from typing import Union

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
