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
from saturn_engine.utils import utcnow
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
            "data": [
                {
                    "loc": ["worker_id"],
                    "msg": "field required",
                    "type": "value_error.missing",
                }
            ],
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

    def create_job(
        name: str,
    ) -> Job:
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

    # Set progress on a job
    jobs_store.update_job(
        session=session,
        name="job-0",
        cursor="1",
    )

    # Create a completed job
    i = i + 1
    create_job(f"job-{i}")
    session.commit()
    jobs_store.update_job(
        session=session,
        name=f"job-{i}",
        cursor="10",
        completed_at=utcnow(),
        error=None,
    )
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
    assert resp.json
    assert ids(resp) == expected_items_worker1
    assert resp.json["items"][0]["state"]["cursor"] == "1"

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
    create_job("job-14")
    session.commit()

    expected_items_worker2.add("job-14")
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

    # job-0 completes and is removed from worker 2's assignations
    jobs_store.update_job(
        session=session,
        name="job-0",
        cursor="10",
        completed_at=utcnow(),
        error=None,
    )
    session.commit()

    resp = client.post("/api/lock", json={"worker_id": "worker-2"})
    assert resp.status_code == 200
    assert ids(resp) == {
        "job-1",
        "job-2",
        "job-3",
        "job-4",
        "job-5",
        "job-6",
        "job-7",
        "job-8",
        "job-9",
        "job-10",
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


def test_ignore_bad_jobs(
    client: FlaskClient,
    session: Session,
    frozen_time: FreezeTime,
    queue_item_maker: Callable[..., api.QueueItem],
    static_definitions: StaticDefinitions,
    fake_job_definition: api.JobDefinition,
) -> None:
    queues_store.create_queue(session=session, name="bogus")
    jobs_store.create_job(
        session=session,
        name="bogus",
        queue_name="bogus",
        job_definition_name="do-not-exists",
    )

    queues_store.create_queue(session=session, name="good")
    jobs_store.create_job(
        session=session,
        name="good",
        queue_name="good",
        job_definition_name=fake_job_definition.name,
    )
    session.commit()

    # Try to lock the queue_item, but job definition do not exists, so skipped.
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.status_code == 200
    assert resp.json
    assert len(resp.json["items"]) == 1
    assert resp.json["items"][0]["name"] == "good"


def test_executors(
    client: FlaskClient,
    session: Session,
    frozen_time: FreezeTime,
    queue_item_maker: Callable[..., api.QueueItem],
    static_definitions: StaticDefinitions,
    fake_job_definition: api.JobDefinition,
) -> None:
    fake_job_definition.template.executor = "ray-executor"
    queues_store.create_queue(session=session, name="test")
    jobs_store.create_job(
        session=session,
        name="test",
        queue_name="test",
        job_definition_name=fake_job_definition.name,
    )
    session.commit()

    # Try to lock the queue_item, but executor is missing, so skipped.
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.status_code == 200
    assert resp.json
    assert not resp.json["items"]
    assert not resp.json["executors"]

    # Add executor to the static definitions.
    static_definitions.executors["ray-executor"] = api.ComponentDefinition(
        name="ray-executor", type="RayExecutor"
    )

    # Now lock return the pipeline with its executor.
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.status_code == 200
    assert resp.json
    assert resp.json["items"][0]["name"] == "test"
    assert resp.json["executors"][0]["name"] == "ray-executor"
