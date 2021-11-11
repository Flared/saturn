from datetime import datetime
from datetime import timedelta
from typing import Callable

import werkzeug.test
from flask.testing import FlaskClient

from saturn_engine.core import api
from saturn_engine.database import Session
from saturn_engine.models import Job
from saturn_engine.models import Queue
from saturn_engine.stores import jobs_store
from saturn_engine.stores import queues_store
from saturn_engine.worker_manager.config.declarative import StaticDefinitions
from tests.conftest import FreezeTime


def ids(response: werkzeug.test.TestResponse) -> set[str]:
    return {i["name"] for i in (response.json or {}).get("items", [])}


def test_api_lock_bad_input(client: FlaskClient) -> None:
    resp = client.post("/api/lock", json={})
    assert resp.status_code == 400
    assert resp.json == {
        "error": {
            "code": "INVALID_INPUT",
            "message": "Invalid input",
            "data": {"worker_id": ["Missing data for required field."]},
        },
    }


def test_api_lock(
    client: FlaskClient,
    session: Session,
    frozen_time: FreezeTime,
    queue_item_maker: Callable[..., api.QueueItem],
) -> None:
    def create_queue(name: str) -> Queue:
        queue = queues_store.create_queue(
            session=session, name=name, spec=queue_item_maker()
        )
        session.flush()
        return queue

    def create_job(name: str) -> Job:
        queue = create_queue(name)
        job = jobs_store.create_job(session=session, queue_name=queue.name)
        return job

    for i in range(7):
        create_queue(f"queue-{i}")

    for i in range(5):
        create_job(f"job-{i}")

    session.commit()

    expected_items_worker1 = {
        "queue-0",
        "queue-1",
        "queue-2",
        "queue-3",
        "queue-4",
        "queue-5",
        "queue-6",
        "job-0",
        "job-1",
        "job-2",
    }

    expected_items_worker2 = {
        "job-3",
        "job-4",
    }

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
    create_queue("queue-7")
    session.commit()

    expected_items_worker2.add("queue-7")
    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert ids(resp) == expected_items_worker2

    # Move 20 minutes in the future
    frozen_time.move_to(datetime.now() + timedelta(minutes=20))

    # Worker 2 picks up all of worker 1's expired items
    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert ids(resp) == {
        "queue-0",
        "queue-1",
        "queue-2",
        "queue-3",
        "queue-4",
        "queue-5",
        "queue-6",
        "job-0",
        "job-1",
        "job-2",
    }


def test_api_lock_with_resources(
    client: FlaskClient,
    session: Session,
    frozen_time: FreezeTime,
    queue_item_maker: Callable[..., api.QueueItem],
    static_definitions: StaticDefinitions,
) -> None:
    queue_item = queue_item_maker()
    queue_item.pipeline.info.resources["key"] = "TestApiKey"
    queues_store.create_queue(session=session, name="test", spec=queue_item)
    resource = api.ResourceItem(name="test", type="TestApiKey", data={})
    static_definitions.resources["test"] = resource
    static_definitions.resources_by_type["TestApiKey"] = [resource]
    session.commit()

    # Get items
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.status_code == 200
    assert resp.json
    assert resp.json["items"][0]["name"] == "test"
    assert resp.json["resources"][0]["name"] == "test"
