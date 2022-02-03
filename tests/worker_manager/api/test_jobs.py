import time
from datetime import timedelta

import werkzeug.test
from flask.testing import FlaskClient
from sqlalchemy.orm import Session

from saturn_engine.core import api
from saturn_engine.stores import jobs_store
from saturn_engine.stores import queues_store
from saturn_engine.utils import utcnow
from saturn_engine.worker_manager.config.declarative import StaticDefinitions
from saturn_engine.worker_manager.config.declarative import load_definitions_from_str
from tests.conftest import FreezeTime


def ids(response: werkzeug.test.TestResponse) -> set[str]:
    return {i["name"] for i in (response.json or {}).get("items", [])}


def test_api_jobs(
    client: FlaskClient,
    session: Session,
    fake_job_definition: api.JobDefinition,
    frozen_time: FreezeTime,
) -> None:
    # Empty
    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert resp.json == {"items": []}

    # Add a job
    queue = queues_store.create_queue(session=session, name="test")
    session.flush()
    job = jobs_store.create_job(
        name=queue.name,
        session=session,
        queue_name=queue.name,
        job_definition_name=fake_job_definition.name,
    )
    session.commit()

    # Contains one job
    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert resp.json == {
        "items": [
            {
                "name": job.name,
                "completed_at": None,
                "cursor": None,
                "error": None,
                "started_at": "2018-01-02T00:00:00+00:00",
            }
        ]
    }


def test_api_job(
    client: FlaskClient,
    session: Session,
    fake_job_definition: api.JobDefinition,
    frozen_time: FreezeTime,
) -> None:
    # Empty
    resp = client.get("/api/job/1")
    assert resp.status_code == 404

    # Add a job
    queue = queues_store.create_queue(session=session, name="test")
    session.flush()
    job = jobs_store.create_job(
        name=queue.name,
        session=session,
        queue_name=queue.name,
        job_definition_name=fake_job_definition.name,
    )
    session.commit()

    # Get the job
    resp = client.get(f"/api/jobs/{job.name}")
    assert resp.status_code == 200
    assert resp.json == {
        "data": {
            "name": job.name,
            "completed_at": None,
            "cursor": None,
            "started_at": "2018-01-02T00:00:00+00:00",
            "error": None,
        }
    }


def test_api_update_job(
    client: FlaskClient,
    session: Session,
    fake_job_definition: api.JobDefinition,
    frozen_time: FreezeTime,
) -> None:
    # Empty
    resp = client.put("/api/job/1")
    assert resp.status_code == 404

    # Add a job
    queue = queues_store.create_queue(session=session, name="test")
    session.flush()
    job = jobs_store.create_job(
        name=queue.name,
        session=session,
        queue_name=queue.name,
        job_definition_name=fake_job_definition.name,
    )
    session.commit()

    # Update the job
    resp = client.put(f"/api/jobs/{job.name}", json={"cursor": "1"})
    assert resp.status_code == 200

    # Get the job
    resp = client.get(f"/api/jobs/{job.name}")
    assert resp.status_code == 200
    assert resp.json == {
        "data": {
            "name": job.name,
            "completed_at": None,
            "cursor": "1",
            "error": None,
            "started_at": "2018-01-02T00:00:00+00:00",
        }
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
            "error": None,
            "started_at": "2018-01-02T00:00:00+00:00",
        }
    }


def test_jobs_sync(
    client: FlaskClient,
    static_definitions: StaticDefinitions,
    session: Session,
    frozen_time: FreezeTime,
) -> None:
    new_definitions_str: str = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnTopic
metadata:
  name: test-topic
spec:
  type: RabbitMQ
  options: {}
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnInventory
metadata:
  name: test-inventory
spec:
  type: testtype
  options: {}
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJobDefinition
metadata:
  name: unscheduled
spec:
  minimalInterval: "@weekly"
  template:
    input:
      inventory: test-inventory
    output:
      default:
        - topic: test-topic
    pipeline:
      name: something.saturn.pipelines.aa.bb
      resources: {"api_key": "GithubApiKey"}
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJobDefinition
metadata:
  name: running
spec:
  minimalInterval: "@weekly"
  template:
    input:
      inventory: test-inventory
    output:
      default:
        - topic: test-topic
    pipeline:
      name: something.saturn.pipelines.aa.bb
      resources: {"api_key": "GithubApiKey"}
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJobDefinition
metadata:
  name: due
spec:
  minimalInterval: "@weekly"
  template:
    input:
      inventory: test-inventory
    output:
      default:
        - topic: test-topic
    pipeline:
      name: something.saturn.pipelines.aa.bb
      resources: {"api_key": "GithubApiKey"}
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJobDefinition
metadata:
  name: not-due
spec:
  minimalInterval: "@weekly"
  template:
    input:
      inventory: test-inventory
    output:
      default:
        - topic: test-topic
    pipeline:
      name: something.saturn.pipelines.aa.bb
      resources: {"api_key": "GithubApiKey"}
"""
    new_definitions = load_definitions_from_str(new_definitions_str)
    static_definitions.job_definitions = new_definitions.job_definitions

    queue = queues_store.create_queue(session=session, name="test")
    jobs_store.create_job(
        name="running",
        session=session,
        queue_name=queue.name,
        job_definition_name="running",
        started_at=utcnow() - timedelta(days=8),
    )
    jobs_store.create_job(
        name="due",
        session=session,
        queue_name=queue.name,
        job_definition_name="due",
        started_at=utcnow() - timedelta(days=8),
        completed_at=utcnow() - timedelta(days=1),
    )
    jobs_store.create_job(
        name="not-due",
        session=session,
        queue_name=queue.name,
        job_definition_name="not-due",
        started_at=utcnow() - timedelta(days=2),
        completed_at=utcnow() - timedelta(days=1),
    )
    session.commit()

    # Empty
    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert resp.json == {
        "items": [
            {
                "completed_at": None,
                "started_at": "2017-12-25T00:00:00+00:00",
                "cursor": None,
                "error": None,
                "name": "running",
            },
            {
                "completed_at": "2018-01-01T00:00:00+00:00",
                "started_at": "2017-12-25T00:00:00+00:00",
                "cursor": None,
                "error": None,
                "name": "due",
            },
            {
                "completed_at": "2018-01-01T00:00:00+00:00",
                "started_at": "2017-12-31T00:00:00+00:00",
                "cursor": None,
                "error": None,
                "name": "not-due",
            },
        ]
    }

    # Trigger sync
    resp = client.post("/api/jobs/sync")
    assert resp.status_code == 200
    assert resp.json == {}

    # Job was created
    expected_response = {
        "items": [
            {
                # Running job was untouched
                "completed_at": None,
                "cursor": None,
                "error": None,
                "name": "running",
                "started_at": "2017-12-25T00:00:00+00:00",
            },
            {
                # The old due job, untouched
                "completed_at": "2018-01-01T00:00:00+00:00",
                "cursor": None,
                "error": None,
                "name": "due",
                "started_at": "2017-12-25T00:00:00+00:00",
            },
            {
                # The not-due job, untouched
                "completed_at": "2018-01-01T00:00:00+00:00",
                "cursor": None,
                "error": None,
                "name": "not-due",
                "started_at": "2017-12-31T00:00:00+00:00",
            },
            {
                # Unscheduled job was scheduled
                "completed_at": None,
                "cursor": None,
                "error": None,
                "name": "unscheduled-1514851200",
                "started_at": "2018-01-02T00:00:00+00:00",
            },
            {
                # The due job was scheduled
                "completed_at": None,
                "cursor": None,
                "error": None,
                "name": "due-1514851200",
                "started_at": "2018-01-02T00:00:00+00:00",
            },
        ]
    }

    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert resp.json == expected_response

    # Trigger again, nothing changes.
    resp = client.post("/api/jobs/sync")
    assert resp.status_code == 200
    assert resp.json == {}

    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert resp.json == expected_response


def test_failed_jobs(
    client: FlaskClient,
    session: Session,
    static_definitions: StaticDefinitions,
    frozen_time: FreezeTime,
) -> None:
    # Add a job
    new_definitions_str: str = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnInventory
metadata:
  name: test-inventory
spec:
  type: testtype
  options: {}
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJobDefinition
metadata:
  name: test
spec:
  minimalInterval: "@weekly"
  template:
    input:
      inventory: test-inventory
    pipeline:
      name: something.saturn.pipelines.aa.bb
"""
    new_definitions = load_definitions_from_str(new_definitions_str)
    static_definitions.job_definitions = new_definitions.job_definitions

    queue = queues_store.create_queue(session=session, name="test")
    session.flush()
    job = jobs_store.create_job(
        name="test",
        session=session,
        queue_name=queue.name,
        job_definition_name="test",
        started_at=utcnow() - timedelta(days=1),
    )
    session.commit()

    # Lock jobs
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.status_code == 200
    assert ids(resp) == {"test"}

    # Fail the job without completing it.
    resp = client.put(
        f"/api/jobs/{job.name}",
        json={
            "cursor": "3",
            "error": "ValueError('oops')",
        },
    )
    assert resp.status_code == 400
    assert resp.json
    assert resp.json["error"]["code"] == "CANNOT_ERROR_UNCOMPLETED_JOB"

    # Fail the job
    resp = client.put(
        f"/api/jobs/{job.name}",
        json={
            "cursor": "3",
            "completed_at": "2018-01-02T00:00:00+00:00",
            "error": "ValueError('oops')",
        },
    )
    assert resp.status_code == 200

    # Lock jobs
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.status_code == 200
    assert not ids(resp)

    # Sync jobs again to spawn a new job.
    resp = client.post("/api/jobs/sync")
    assert resp.status_code == 200
    assert resp.json == {}

    # Lock jobs
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    new_job_name = f"test-{int(time.time())}"
    assert resp.status_code == 200
    assert ids(resp) == {new_job_name}

    # Cursor should resume where it was at.
    resp = client.get(f"/api/jobs/{new_job_name}")
    assert resp.status_code == 200
    assert resp.json and resp.json["data"]["cursor"] == "3"


def test_saturn_jobs_sync(
    client: FlaskClient,
    static_definitions: StaticDefinitions,
    session: Session,
    frozen_time: FreezeTime,
) -> None:
    """test for jobs that don't have an interval (saturn jobs)"""

    new_definitions_str: str = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnTopic
metadata:
  name: test-topic
spec:
  type: RabbitMQ
  options: {}
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnInventory
metadata:
  name: test-inventory
spec:
  type: testtype
  options: {}
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJob
metadata:
  name: test-job
spec:
  input:
    inventory: test-inventory
  output:
    default:
      - topic: test-topic
  pipeline:
    name: something.saturn.pipelines.aa.bb
    resources: {"api_key": "GithubApiKey"}
"""
    new_definitions = load_definitions_from_str(new_definitions_str)
    static_definitions.jobs = new_definitions.jobs

    # Trigger sync
    resp = client.post("/api/jobs/sync")
    assert resp.status_code == 200
    assert resp.json == {}

    # Make sure our job is here
    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    assert resp.json == {
        "items": [
            {
                "completed_at": None,
                "started_at": "2018-01-02T00:00:00+00:00",
                "cursor": None,
                "error": None,
                "name": "test-job",
            }
        ]
    }
