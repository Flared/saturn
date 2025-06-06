import typing as t

import time
from datetime import timedelta

import pytest
import werkzeug.test
from flask.testing import FlaskClient
from pytest_mock import MockerFixture
from sqlalchemy import select
from sqlalchemy.orm import Session

from saturn_engine.core import api
from saturn_engine.models import Job
from saturn_engine.models import JobCursorState
from saturn_engine.stores import jobs_store
from saturn_engine.stores import queues_store
from saturn_engine.utils import utcnow
from saturn_engine.utils.inspect import get_import_name
from saturn_engine.worker_manager.app import SaturnApp
from saturn_engine.worker_manager.config.declarative import StaticDefinitions
from saturn_engine.worker_manager.config.declarative import load_definitions_from_str
from saturn_engine.worker_manager.context import _load_static_definition
from tests.conftest import FreezeTime


@pytest.fixture
def fake_job(
    session: Session,
    fake_job_definition: api.JobDefinition,
    frozen_time: FreezeTime,
) -> Job:
    queue = queues_store.create_queue(session=session, name="test-1")
    session.flush()
    job = jobs_store.create_job(
        name=queue.name,
        session=session,
        queue_name=queue.name,
        job_definition_name=fake_job_definition.name,
    )
    session.commit()
    return job


@pytest.fixture
def new_job(
    fake_job: Job,
    session: Session,
    frozen_time: FreezeTime,
) -> Job:
    queue = queues_store.create_queue(session=session, name="test-2")
    job = jobs_store.create_job(
        name=queue.name,
        session=session,
        queue_name=queue.name,
        job_definition_name=fake_job.job_definition_name,
    )
    session.commit()
    return job


@pytest.fixture
def orphan_job(
    session: Session,
    frozen_time: FreezeTime,
) -> Job:
    queue = queues_store.create_queue(session=session, name="orphan-test")
    job = jobs_store.create_job(
        name=queue.name,
        session=session,
        queue_name=queue.name,
    )
    session.commit()
    return job


@pytest.fixture
def mock_definitions(mocker: MockerFixture) -> t.Callable[[StaticDefinitions], None]:
    def mock_definitions(definitions: StaticDefinitions) -> None:
        mocker.patch(
            get_import_name(_load_static_definition), new=lambda *x, **y: definitions
        )

    return mock_definitions


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
                "enabled": True,
                "assigned_to": None,
                "assigned_at": None,
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
            "enabled": True,
            "assigned_to": None,
            "assigned_at": None,
        }
    }


def test_api_update_job(
    client: FlaskClient,
    fake_job: Job,
    frozen_time: FreezeTime,
) -> None:
    # Update the job
    resp = client.put(f"/api/jobs/{fake_job.name}", json={"cursor": "1"})
    assert resp.status_code == 200

    # Get the job
    resp = client.get(f"/api/jobs/{fake_job.name}")
    assert resp.status_code == 200
    assert resp.json == {
        "data": {
            "name": fake_job.name,
            "completed_at": None,
            "cursor": "1",
            "error": None,
            "started_at": "2018-01-02T00:00:00+00:00",
            "enabled": True,
            "assigned_to": None,
            "assigned_at": None,
        }
    }

    # Complete the job
    resp = client.put(
        f"/api/jobs/{fake_job.name}",
        json={"cursor": "2", "completed_at": "2018-01-02T00:00:00+00:00"},
    )
    assert resp.status_code == 200

    # Get the job
    resp = client.get(f"/api/jobs/{fake_job.name}")
    assert resp.status_code == 200
    assert resp.json == {
        "data": {
            "name": fake_job.name,
            "completed_at": "2018-01-02T00:00:00+00:00",
            "cursor": "2",
            "error": None,
            "started_at": "2018-01-02T00:00:00+00:00",
            "enabled": False,
            "assigned_to": None,
            "assigned_at": None,
        }
    }


def test_jobs_sync(
    client: FlaskClient,
    app: SaturnApp,
    static_definitions: StaticDefinitions,
    session: Session,
    frozen_time: FreezeTime,
    fake_executor: api.ComponentDefinition,
    mock_definitions: t.Callable[[StaticDefinitions], None],
) -> None:
    new_definitions_str: str = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnTopic
metadata:
  name: test-topic
spec:
  type: RabbitMQTopic
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
  labels:
    owner: team-saturn
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
  labels:
    owner: team-saturn
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
  labels:
    owner: team-saturn
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
  labels:
    owner: team-saturn
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

    mock_definitions(static_definitions)

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
                "enabled": True,
                "assigned_to": None,
                "assigned_at": None,
            },
            {
                "completed_at": "2018-01-01T00:00:00+00:00",
                "started_at": "2017-12-25T00:00:00+00:00",
                "cursor": None,
                "error": None,
                "name": "due",
                "enabled": True,
                "assigned_to": None,
                "assigned_at": None,
            },
            {
                "completed_at": "2018-01-01T00:00:00+00:00",
                "started_at": "2017-12-31T00:00:00+00:00",
                "cursor": None,
                "error": None,
                "name": "not-due",
                "enabled": True,
                "assigned_to": None,
                "assigned_at": None,
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
                "enabled": True,
                "assigned_to": None,
                "assigned_at": None,
            },
            {
                # The old due job, untouched
                "completed_at": "2018-01-01T00:00:00+00:00",
                "cursor": None,
                "error": None,
                "name": "due",
                "started_at": "2017-12-25T00:00:00+00:00",
                "enabled": True,
                "assigned_to": None,
                "assigned_at": None,
            },
            {
                # The not-due job, untouched
                "completed_at": "2018-01-01T00:00:00+00:00",
                "cursor": None,
                "error": None,
                "name": "not-due",
                "started_at": "2017-12-31T00:00:00+00:00",
                "enabled": True,
                "assigned_to": None,
                "assigned_at": None,
            },
            {
                # Unscheduled job was scheduled
                "completed_at": None,
                "cursor": None,
                "error": None,
                "name": "unscheduled-1514851200",
                "started_at": "2018-01-02T00:00:00+00:00",
                "enabled": True,
                "assigned_to": None,
                "assigned_at": None,
            },
            {
                # The due job was scheduled
                "completed_at": None,
                "cursor": None,
                "error": None,
                "name": "due-1514851200",
                "started_at": "2018-01-02T00:00:00+00:00",
                "enabled": True,
                "assigned_to": None,
                "assigned_at": None,
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
    fake_executor: api.ComponentDefinition,
    frozen_time: FreezeTime,
    mock_definitions: t.Callable[[StaticDefinitions], None],
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
  labels:
    owner: team-saturn
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

    mock_definitions(static_definitions)

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
    mock_definitions: t.Callable[[StaticDefinitions], None],
) -> None:
    """test for jobs that don't have an interval (saturn jobs)"""

    new_definitions_str: str = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnTopic
metadata:
  name: test-topic
spec:
  type: RabbitMQTopic
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
  labels:
    owner: team-saturn
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

    mock_definitions(static_definitions)

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
                "enabled": True,
                "assigned_to": None,
                "assigned_at": None,
            }
        ]
    }


def test_sync_states(
    client: FlaskClient,
    session: Session,
    fake_job: Job,
    new_job: Job,
    orphan_job: Job,
    fake_job_definition: api.JobDefinition,
    frozen_time: FreezeTime,
) -> None:
    resp = client.post(
        "/api/jobs/_states",
        json={
            "state": {
                "jobs": {
                    fake_job.name: {
                        "cursor": "1",
                        "cursors_states": {
                            "a": {"x": 1},
                            "b": {"x": 2},
                        },
                        "completion": {
                            "completed_at": "2020-01-01T01:02:03+04:00",
                            "error": "ValueError: boo",
                        },
                    },
                }
            }
        },
    )
    assert resp.status_code == 200
    assert resp.json == {}

    resp = client.post(
        "/api/jobs/_states",
        json={
            "state": {
                "jobs": {
                    new_job.name: {
                        "cursors_states": {
                            "b": {"x": 3},
                        },
                    },
                    orphan_job.name: {"cursors_states": {"a": {"x": 1}}},
                },
            }
        },
    )
    assert resp.status_code == 200
    assert resp.json == {}

    database_state = session.scalars(
        select(JobCursorState).order_by(
            JobCursorState.job_definition_name, JobCursorState.cursor
        )
    ).all()
    assert [
        {"job": r.job_definition_name, "cursor": r.cursor, "state": r.state}
        for r in database_state
    ] == [
        {"job": "orphan-test", "cursor": "a", "state": {"x": 1}},
        {"job": "test", "cursor": "a", "state": {"x": 1}},
        {"job": "test", "cursor": "b", "state": {"x": 3}},
    ]


def test_fetch_cursors_states(
    client: FlaskClient,
    fake_job: Job,
    new_job: Job,
    orphan_job: Job,
) -> None:
    custom_ns = "custom-namespace"
    resp = client.post(
        "/api/jobs/_states",
        json={
            "state": {
                "jobs": {
                    fake_job.name: {
                        "cursors_states": {
                            "a": {"x": 1},
                            "b": {"x": 2},
                        },
                    },
                    custom_ns: {
                        "cursors_states": {
                            "c": {"x": 1},
                        }
                    },
                }
            }
        },
    )
    assert resp.status_code == 200
    assert resp.json == {}

    resp = client.post(
        "/api/jobs/_states/fetch",
        json={
            "cursors": {
                new_job.name: ["a", "c"],
                orphan_job.name: ["a"],
                "do-not-exist": ["a"],
                custom_ns: ["c", "d"],
            }
        },
    )
    assert resp.status_code == 200
    assert resp.json == {
        "cursors": {
            new_job.name: {
                "a": {"x": 1},
                "c": None,
            },
            orphan_job.name: {"a": None},
            "do-not-exist": {"a": None},
            custom_ns: {
                "c": {"x": 1},
                "d": None,
            },
        }
    }


def test_start_job_without_restart(
    client: FlaskClient,
    session: Session,
    fake_job_definition: api.JobDefinition,
    static_definitions: StaticDefinitions,
    frozen_time: FreezeTime,
) -> None:
    resp = client.post("/api/jobs/_start", json={})
    assert resp.status_code == 400
    assert resp.json
    assert resp.json["error"]["code"] == "MUST_SPECIFY_JOB"

    resp = client.post(
        "/api/jobs/_start",
        json={"name": "name", "job_definition_name": "job_definition_name"},
    )
    assert resp.status_code == 400
    assert resp.json
    assert resp.json["error"]["code"] == "MUST_SPECIFY_ONE_JOB"

    resp = client.post("/api/jobs/_start", json={"name": "try to find this"})
    assert resp.status_code == 400
    assert resp.json
    assert resp.json["error"]["code"] == "JOB_START_ERROR"

    queue = queues_store.create_queue(session=session, name="fake-queue")
    session.flush()
    first_job = jobs_store.create_job(
        name=queue.name,
        session=session,
        queue_name=queue.name,
        job_definition_name=fake_job_definition.name,
    )
    session.commit()

    frozen_time.tick()

    resp = client.post("/api/jobs/_start", json={"name": first_job.name})
    assert resp.status_code == 400
    assert resp.json
    assert resp.json["error"]["code"] == "JOB_START_ERROR"

    jobs_store.update_job(session=session, name=first_job.name, completed_at=utcnow())
    session.commit()

    resp = client.post("/api/jobs/_start", json={"name": first_job.name})
    assert resp.status_code == 200
    assert resp.json

    second_job = jobs_store.get_job(session=session, name=resp.json["name"])
    assert second_job

    jobs_store.update_job(session=session, name=second_job.name, completed_at=utcnow())
    session.commit()

    frozen_time.tick()

    resp = client.post(
        "/api/jobs/_start", json={"job_definition_name": fake_job_definition.name}
    )

    assert resp.status_code == 200
    assert resp.json

    third_job = jobs_store.get_job(session=session, name=resp.json["name"])
    assert third_job


def test_start_job_with_restart(
    client: FlaskClient,
    session: Session,
    fake_job_definition: api.JobDefinition,
    frozen_time: FreezeTime,
) -> None:
    queue = queues_store.create_queue(session=session, name="fake-queue")
    session.flush()
    first_job = jobs_store.create_job(
        name=queue.name,
        session=session,
        queue_name=queue.name,
        job_definition_name=fake_job_definition.name,
    )
    session.commit()

    frozen_time.tick()

    resp = client.post("/api/jobs/_start?restart=true", json={"name": first_job.name})

    assert resp.status_code == 200
    assert resp.json

    second_job = jobs_store.get_job(session=session, name=resp.json["name"])
    session.refresh(first_job)

    assert second_job
    assert first_job.completed_at is not None
    assert first_job.error == "Cancelled"

    frozen_time.tick()

    resp = client.post(
        "/api/jobs/_start?restart=true",
        json={"job_definition_name": fake_job_definition.name},
    )
    assert resp.status_code == 200
    assert resp.json

    third_job = jobs_store.get_job(session=session, name=resp.json["name"])
    session.refresh(second_job)

    assert third_job
    assert second_job.completed_at is not None
    assert second_job.error == "Cancelled"
