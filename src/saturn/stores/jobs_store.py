from typing import Union

from saturn.database import AnySession
from saturn.models import Job


def create_job(session: Union[AnySession]) -> Job:
    job = Job()
    session.add(job)
    return job
