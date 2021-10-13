from datetime import datetime
from datetime import timedelta

from flask.testing import FlaskClient

from saturn_engine.database import Session
from saturn_engine.models import Job
from saturn_engine.models import Queue
from saturn_engine.stores import jobs_store
from saturn_engine.stores import queues_store
from tests.conftest import FreezeTime


def test_api_lock_bad_input(client: FlaskClient) -> None:
    resp = client.post("/api/lock", json={})
    assert resp.status_code == 400
    assert resp.json == {
        "error": {
            "code": "BAD_LOCK_INPUT",
            "message": "Bad lock input",
            "data": {"worker_id": ["Missing data for required field."]},
        },
    }


def test_api_lock(
    client: FlaskClient,
    session: Session,
    frozen_time: FreezeTime,
) -> None:
    def create_queue() -> Queue:
        queue = queues_store.create_queue(session=session, pipeline="test")
        session.flush()
        return queue

    def create_job() -> Job:
        queue = create_queue()
        job = jobs_store.create_job(session=session, queue_id=queue.id)
        return job

    for _ in range(7):
        create_queue()

    for _ in range(5):
        create_job()

    session.commit()

    expected_items_worker1 = {
        "items": [
            {"queue": {"id": 1, "pipeline": "test"}},
            {"queue": {"id": 2, "pipeline": "test"}},
            {"queue": {"id": 3, "pipeline": "test"}},
            {"queue": {"id": 4, "pipeline": "test"}},
            {"queue": {"id": 5, "pipeline": "test"}},
            {"queue": {"id": 6, "pipeline": "test"}},
            {"queue": {"id": 7, "pipeline": "test"}},
            {"job": {"id": 8, "pipeline": "test"}},
            {"job": {"id": 9, "pipeline": "test"}},
            {"job": {"id": 10, "pipeline": "test"}},
        ]
    }

    expected_items_worker2 = {
        "items": [
            {"job": {"id": 11, "pipeline": "test"}},
            {"job": {"id": 12, "pipeline": "test"}},
        ]
    }

    # Get items
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.status_code == 200
    assert resp.json == expected_items_worker1

    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert resp.json == expected_items_worker2

    # Verify assignations still stand.
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.status_code == 200
    assert resp.json == expected_items_worker1

    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert resp.json == expected_items_worker2

    # Create new work. It should be picked by worker2.
    create_queue()
    session.commit()

    expected_items_worker2 = {
        "items": (
            expected_items_worker2["items"]
            + [{"queue": {"id": 13, "pipeline": "test"}}]
        )
    }
    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert resp.json == expected_items_worker2

    # Move 20 minutes in the future
    frozen_time.move_to(datetime.now() + timedelta(minutes=20))  # type: ignore

    # Worker 2 picks up all of worker 1's expired items
    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert resp.json == {
        "items": [
            {"queue": {"id": 1, "pipeline": "test"}},
            {"queue": {"id": 2, "pipeline": "test"}},
            {"queue": {"id": 3, "pipeline": "test"}},
            {"queue": {"id": 4, "pipeline": "test"}},
            {"queue": {"id": 5, "pipeline": "test"}},
            {"queue": {"id": 6, "pipeline": "test"}},
            {"queue": {"id": 7, "pipeline": "test"}},
            {"job": {"id": 8, "pipeline": "test"}},
            {"job": {"id": 9, "pipeline": "test"}},
            {"job": {"id": 10, "pipeline": "test"}},
        ]
    }
