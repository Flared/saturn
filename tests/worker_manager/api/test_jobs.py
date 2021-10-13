from flask.testing import FlaskClient
from sqlalchemy.orm import Session

from saturn_engine.stores import jobs_store
from saturn_engine.stores import queues_store


def test_api_jobs(client: FlaskClient, session: Session) -> None:
    # Empty
    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert resp.json == {"jobs": []}

    # Add a job
    queue = queues_store.create_queue(session=session, pipeline="test")
    session.flush()
    jobs_store.create_job(session=session, queue_id=queue.id)
    session.commit()

    # Contains one job
    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert resp.json == {"jobs": [{"id": 1, "completed_at": None}]}
