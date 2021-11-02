import os

from saturn_engine.core.api import InventoryItem
from saturn_engine.core.api import JobDefinition
from saturn_engine.core.api import TopicItem
from saturn_engine.worker_manager.config.declarative import (
    load_definitions_from_directory,
)
from saturn_engine.worker_manager.config.declarative import load_definitions_from_str


def test_load_definitions_from_directory_simple() -> None:
    test_dir = os.path.join(
        os.path.dirname(__file__),
        "testdata",
        "test_declarative_simple",
    )
    static_definitions = load_definitions_from_directory(test_dir)
    assert (
        static_definitions.inventories["github-identifiers"].name
        == "github-identifiers"
    )


def test_load_job_definition() -> None:
    job_definition_str: str = """
apiVersion: saturn.github.io/v1alpha1
kind: SaturnTopic
metadata:
  name: test-topic
spec:
  type: RabbitMQ
  options: {}
---
apiVersion: saturn.github.io/v1alpha1
kind: SaturnInventory
metadata:
  name: test-inventory
spec:
  type: testtype
  options: {}
---
apiVersion: saturn.github.io/v1alpha1
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
    static_definitions = load_definitions_from_str(job_definition_str)

    assert (
        static_definitions.job_definitions["test-job-definition"].name
        == "test-job-definition"
    )
    assert (
        static_definitions.job_definitions["test-job-definition"].minimal_interval
        == "@weekly"
    )

    assert isinstance(static_definitions.inventories["test-inventory"], InventoryItem)
    assert isinstance(static_definitions.topics["test-topic"], TopicItem)
    assert isinstance(
        static_definitions.job_definitions["test-job-definition"], JobDefinition
    )


def test_load_job_definition_unordered() -> None:
    # We can define objects in any order.
    # Its fine to define jobsdefinitions before the topic that they refer to.
    job_definition_str: str = """
---
apiVersion: saturn.github.io/v1alpha1
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
---
apiVersion: saturn.github.io/v1alpha1
kind: SaturnTopic
metadata:
  name: test-topic
spec:
  type: RabbitMQ
  options: {}
---
apiVersion: saturn.github.io/v1alpha1
kind: SaturnInventory
metadata:
  name: test-inventory
spec:
  type: testtype
  options: {}
---

"""
    static_definitions = load_definitions_from_str(job_definition_str)

    assert len(static_definitions.inventories) == 1
    assert len(static_definitions.job_definitions) == 1
    assert len(static_definitions.topics) == 1
