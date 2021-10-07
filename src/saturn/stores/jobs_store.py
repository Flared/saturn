from sqlalchemy.orm import Session

from saturn.models import Job


def create_job(session: Session) -> Job:
    job = Job()
    session.add(job)
    return job
