from datetime import datetime
from datetime import timedelta

import werkzeug.test
from flask.testing import FlaskClient

from saturn_engine.database import Session
from saturn_engine.models import Job
from saturn_engine.models import Queue
from saturn_engine.stores import jobs_store
from saturn_engine.stores import queues_store
from tests.conftest import FreezeTime


def ids(response: werkzeug.test.TestResponse) -> list[str]:
    return [i["name"] for i in (response.json or {}).get("items", [])]


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

    expected_items_worker1 = [
        "queue-1",
        "queue-2",
        "queue-3",
        "queue-4",
        "queue-5",
        "queue-6",
        "queue-7",
        "job-8",
        "job-9",
        "job-10",
    ]

    expected_items_worker2 = [
        "job-11",
        "job-12",
    ]

    # Get items
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.status_code == 200
    assert ids(resp) == expected_items_worker1

    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert ids(resp) == expected_items_worker2

    # Verify assignations still stand.
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.status_code == 200
    assert ids(resp) == expected_items_worker1

    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert ids(resp) == expected_items_worker2

    # Create new work. It should be picked by worker2.
    create_queue()
    session.commit()

    expected_items_worker2.append("queue-13")
    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert ids(resp) == expected_items_worker2

    # Move 20 minutes in the future
    frozen_time.move_to(datetime.now() + timedelta(minutes=20))

    # Worker 2 picks up all of worker 1's expired items
    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert ids(resp) == [
        "queue-1",
        "queue-2",
        "queue-3",
        "queue-4",
        "queue-5",
        "queue-6",
        "queue-7",
        "job-8",
        "job-9",
        "job-10",
    ]
