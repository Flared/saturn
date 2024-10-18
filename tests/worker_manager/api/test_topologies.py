from unittest import mock

from flask.testing import FlaskClient
from sqlalchemy.orm import Session

from saturn_engine.worker_manager.app import SaturnApp


def test_put_topology_patch(client: FlaskClient) -> None:
    resp = client.put(
        "/api/topologies/patch",
        json={
            "apiVersion": "saturn.flared.io/v1alpha1",
            "kind": "SaturnTopic",
            "metadata": {"name": "test-topic"},
            "spec": {"type": "RabbitMQTopic", "options": {"queue_name": "queue_1"}},
        },
    )
    assert resp.status_code == 200
    assert resp.json == {
        "apiVersion": "saturn.flared.io/v1alpha1",
        "kind": "SaturnTopic",
        "metadata": {"name": "test-topic", "labels": {}},
        "spec": {"type": "RabbitMQTopic", "options": {"queue_name": "queue_1"}},
    }

    # We add a new patch, overriding the last one
    resp = client.put(
        "/api/topologies/patch",
        json={
            "apiVersion": "saturn.flared.io/v1alpha1",
            "kind": "SaturnTopic",
            "metadata": {"name": "test-topic"},
            "spec": {"type": "RabbitMQTopic", "options": {"queue_name": "queue_2"}},
        },
    )
    assert resp.status_code == 200
    assert resp.json == {
        "apiVersion": "saturn.flared.io/v1alpha1",
        "kind": "SaturnTopic",
        "metadata": {"name": "test-topic", "labels": {}},
        "spec": {"type": "RabbitMQTopic", "options": {"queue_name": "queue_2"}},
    }


def test_put_topology_patch_ensure_topology_changed(
    tmp_path: str, app: SaturnApp, client: FlaskClient, session: Session
) -> None:
    topology = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnExecutor
metadata:
  name: default
spec:
  type: ARQExecutor
  options:
    redis_url: "redis://redis"
    queue_name: "arq:saturn-default"
    redis_pool_args:
      max_connections: 10000
    concurrency: 108
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnInventory
metadata:
    name: test-inventory
spec:
    type: testtype
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJobDefinition
metadata:
    name: job_1
    labels:
      owner: team-saturn
spec:
  minimalInterval: "@weekly"
  template:
    input:
      inventory: test-inventory
    pipeline:
      name: something.saturn.pipelines.aa.bb
---
    """
    with open(f"{tmp_path}/topology.yaml", "+w") as f:
        f.write(topology)

    app.saturn.config.static_definitions_directories = [tmp_path]
    app.saturn.load_static_definition(session=session)

    resp = client.post("/api/jobs/sync")
    assert resp.status_code == 200
    assert resp.json == {}
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.json == {
        "executors": [
            {
                "name": "default",
                "options": {
                    "concurrency": 108,
                    "queue_name": "arq:saturn-default",
                    "redis_pool_args": {"max_connections": 10000},
                    "redis_url": "redis://redis",
                },
                "type": "ARQExecutor",
            }
        ],
        "items": [
            {
                "config": {},
                "executor": "default",
                "input": {"name": "test-inventory", "options": {}, "type": "testtype"},
                "labels": {
                    "owner": "team-saturn",
                    "internal.job-definition-name": "job_1",
                },
                "name": mock.ANY,
                "output": {},
                "pipeline": {
                    "args": {},
                    "info": {
                        "name": "something.saturn.pipelines.aa.bb",
                        "resources": {},
                    },
                },
                "state": {
                    "cursor": None,
                    "started_at": mock.ANY,
                },
            }
        ],
        "resources": [],
        "resources_providers": [],
    }

    # Let's change the pipeline name
    resp = client.put(
        "/api/topologies/patch",
        json={
            "apiVersion": "saturn.flared.io/v1alpha1",
            "kind": "SaturnJobDefinition",
            "metadata": {"name": "job_1"},
            "spec": {
                "template": {
                    "pipeline": {"name": "something.else.saturn.pipelines.aa.bb"},
                },
            },
        },
    )

    # And reset the static definition
    session.commit()
    app.saturn.load_static_definition(session=session)

    # Make sure we have the new topology version
    resp = client.post("/api/lock", json={"worker_id": "worker-1"})
    assert resp.json == {
        "executors": [
            {
                "name": "default",
                "options": {
                    "concurrency": 108,
                    "queue_name": "arq:saturn-default",
                    "redis_pool_args": {"max_connections": 10000},
                    "redis_url": "redis://redis",
                },
                "type": "ARQExecutor",
            }
        ],
        "items": [
            {
                "config": {},
                "executor": "default",
                "input": {"name": "test-inventory", "options": {}, "type": "testtype"},
                "labels": {
                    "owner": "team-saturn",
                    "internal.job-definition-name": "job_1",
                },
                "name": mock.ANY,
                "output": {},
                "pipeline": {
                    "args": {},
                    "info": {
                        "name": "something.else.saturn.pipelines.aa.bb",
                        "resources": {},
                    },
                },
                "state": {
                    "cursor": None,
                    "started_at": mock.ANY,
                },
            }
        ],
        "resources": [],
        "resources_providers": [],
    }
