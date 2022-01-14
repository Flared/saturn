from typing import Callable

from datetime import datetime
from datetime import timedelta

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
    fake_job_definition: api.JobDefinition,
) -> None:
    def create_queue(name: str) -> Queue:
        queue = queues_store.create_queue(session=session, name=name)
        session.flush()
        return queue

    def create_job(name: str) -> Job:
        queue = create_queue(name)
        job = jobs_store.create_job(
            name=queue.name,
            session=session,
            queue_name=queue.name,
            job_definition_name=fake_job_definition.name,
        )
        return job

    for i in range(13):
        create_job(f"job-{i}")

    session.commit()

    expected_items_worker1 = {
        "job-0",
        "job-1",
        "job-2",
        "job-3",
        "job-4",
        "job-5",
        "job-6",
        "job-7",
        "job-8",
        "job-9",
    }

    expected_items_worker2 = {
        "job-10",
        "job-11",
        "job-12",
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
    create_job("job-13")
    session.commit()

    expected_items_worker2.add("job-13")
    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert ids(resp) == expected_items_worker2

    # Move 20 minutes in the future
    frozen_time.move_to(datetime.now() + timedelta(minutes=20))

    # Worker 2 picks up all of worker 1's expired items
    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert ids(resp) == {
        "job-0",
        "job-1",
        "job-2",
        "job-3",
        "job-4",
        "job-5",
        "job-6",
        "job-7",
        "job-8",
        "job-9",
    }


def test_api_lock_with_resources(
    client: FlaskClient,
    session: Session,
    frozen_time: FreezeTime,
    queue_item_maker: Callable[..., api.QueueItem],
    static_definitions: StaticDefinitions,
    fake_job_definition: api.JobDefinition,
) -> None:
    queue_item = queue_item_maker()
    queue_item.pipeline.info.resources["key"] = "TestApiKey"
    queues_store.create_queue(session=session, name="test")
    jobs_store.create_job(
        session=session,
        name="test",
        queue_name="test",
        job_definition_name=fake_job_definition.name,
    )
    session.commit()

    # Try to lock the queue_item, but resources are missing, so skipped.
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.status_code == 200
    assert resp.json
    assert not resp.json["items"]
    assert not resp.json["resources"]

    # Add resources to the static definitions.
    resource = api.ResourceItem(name="test", type="TestApiKey", data={})
    static_definitions.resources["test"] = resource
    static_definitions.resources_by_type["TestApiKey"] = [resource]

    # Now lock return the pipeline with its resource.
    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert resp.json
    assert resp.json["items"][0]["name"] == "test"
    assert resp.json["resources"][0]["name"] == "test"
