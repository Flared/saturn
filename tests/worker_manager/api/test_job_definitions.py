from flask.testing import FlaskClient

from saturn_engine.worker_manager.config.declarative import StaticDefinitions
from saturn_engine.worker_manager.config.declarative import load_definitions_from_str


def test_api_job_definitions_empty(client: FlaskClient) -> None:
    resp = client.get("/api/job_definitions")
    assert resp.status_code == 200
    assert resp.json == {"items": []}


def test_api_job_definitions_loaded_from_str(
    client: FlaskClient,
    static_definitions: StaticDefinitions,
) -> None:
    new_definitions = load_definitions_from_str(
        """
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
  name: test-job-definition
spec:
  minimalInterval: "@weekly"
  template:
    name: test

    input:
      inventory: test-inventory

    output:
      default:
        - topic: test-topic

    pipeline:
      name: something.saturn.pipelines.aa.bb
      resources: {"api_key": "GithubApiKey"}
"""
    )
    static_definitions.job_definitions = new_definitions.job_definitions
    resp = client.get("/api/job_definitions")
    assert resp.status_code == 200
    assert resp.json == {
        "items": [
            {
                "minimal_interval": "@weekly",
                "name": "test-job-definition",
                "template": {
                    "input": {
                        "inventory": {
                            "name": "test-inventory",
                            "options": {},
                            "type": "testtype",
                        },
                    },
                    "name": "test",
                    "output": {
                        "default": [
                            {"name": "test-topic", "options": {}, "type": "RabbitMQ"}
                        ]
                    },
                    "pipeline": {
                        "args": {},
                        "info": {
                            "name": "something.saturn.pipelines.aa.bb",
                            "resources": {"api_key": "GithubApiKey"},
                        },
                    },
                },
            }
        ]
    }
