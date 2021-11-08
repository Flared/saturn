from flask.testing import FlaskClient
from sqlalchemy.orm import Session

from saturn_engine.stores import jobs_store
from saturn_engine.stores import queues_store


def test_api_jobs(client: FlaskClient, session: Session) -> None:
    # Empty
    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert resp.json == {"items": []}

    # Add a job
    queue = queues_store.create_queue(session=session, pipeline="test")
    session.flush()
    job = jobs_store.create_job(session=session, queue_id=queue.id)
    session.commit()

    # Contains one job
    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert resp.json == {
        "items": [{"id": job.id, "completed_at": None, "cursor": None}]
    }


def test_api_job(client: FlaskClient, session: Session) -> None:
    # Empty
    resp = client.get("/api/job/1")
    assert resp.status_code == 404

    # Add a job
    queue = queues_store.create_queue(session=session, pipeline="test")
    session.flush()
    job = jobs_store.create_job(session=session, queue_id=queue.id)
    session.commit()

    # Get the job
    resp = client.get(f"/api/jobs/{job.id}")
    assert resp.status_code == 200
    assert resp.json == {"data": {"id": job.id, "completed_at": None, "cursor": None}}


def test_api_upodate_job(client: FlaskClient, session: Session) -> None:
    # Empty
    resp = client.put("/api/job/1")
    assert resp.status_code == 404

    # Add a job
    queue = queues_store.create_queue(session=session, pipeline="test")
    session.flush()
    job = jobs_store.create_job(session=session, queue_id=queue.id)
    session.commit()

    # Update the job
    resp = client.put(f"/api/jobs/{job.id}", json={"cursor": "1"})
    assert resp.status_code == 200

    # Get the job
    resp = client.get(f"/api/jobs/{job.id}")
    assert resp.status_code == 200
    assert resp.json == {"data": {"id": job.id, "completed_at": None, "cursor": "1"}}
