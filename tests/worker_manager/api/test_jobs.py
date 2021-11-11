from typing import Callable

from flask.testing import FlaskClient
from sqlalchemy.orm import Session

from saturn_engine.core.api import QueueItem
from saturn_engine.stores import jobs_store
from saturn_engine.stores import queues_store


def test_api_jobs(
    client: FlaskClient, session: Session, queue_item_maker: Callable[..., QueueItem]
) -> None:
    # Empty
    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert resp.json == {"items": []}

    # Add a job
    queue = queues_store.create_queue(
        session=session, name="test", spec=queue_item_maker()
    )
    session.flush()
    job = jobs_store.create_job(session=session, queue_name=queue.name)
    session.commit()

    # Contains one job
    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert resp.json == {
        "items": [{"name": job.name, "completed_at": None, "cursor": None}]
    }


def test_api_job(
    client: FlaskClient, session: Session, queue_item_maker: Callable[..., QueueItem]
) -> None:
    # Empty
    resp = client.get("/api/job/1")
    assert resp.status_code == 404

    # Add a job
    queue = queues_store.create_queue(
        session=session, name="test", spec=queue_item_maker()
    )
    session.flush()
    job = jobs_store.create_job(session=session, queue_name=queue.name)
    session.commit()

    # Get the job
    resp = client.get(f"/api/jobs/{job.name}")
    assert resp.status_code == 200
    assert resp.json == {
        "data": {"name": job.name, "completed_at": None, "cursor": None}
    }


def test_api_update_job(
    client: FlaskClient, session: Session, queue_item_maker: Callable[..., QueueItem]
) -> None:
    # Empty
    resp = client.put("/api/job/1")
    assert resp.status_code == 404

    # Add a job
    queue = queues_store.create_queue(
        session=session, name="test", spec=queue_item_maker()
    )
    session.flush()
    job = jobs_store.create_job(session=session, queue_name=queue.name)
    session.commit()

    # Update the job
    resp = client.put(f"/api/jobs/{job.name}", json={"cursor": "1"})
    assert resp.status_code == 200

    # Get the job
    resp = client.get(f"/api/jobs/{job.name}")
    assert resp.status_code == 200
    assert resp.json == {
        "data": {"name": job.name, "completed_at": None, "cursor": "1"}
    }

    # Complete the job
    resp = client.put(
        f"/api/jobs/{job.name}",
        json={"cursor": "2", "completed_at": "2018-01-02T00:00:00+00:00"},
    )
    assert resp.status_code == 200

    # Get the job
    resp = client.get(f"/api/jobs/{job.name}")
    assert resp.status_code == 200
    assert resp.json == {
        "data": {
            "name": job.name,
            "completed_at": "2018-01-02T00:00:00+00:00",
            "cursor": "2",
        }
    }
