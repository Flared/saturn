import os

import pytest

from saturn_engine.core.api import InventoryItem
from saturn_engine.core.api import JobDefinition
from saturn_engine.core.api import ResourceItem
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
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnResource
metadata:
  name: test-resource
spec:
  type: TestApiKey
  data:
    key: "qwe"
  default_delay: 10
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnTopic
metadata:
  name: test-topic
spec:
  type: RabbitMQTopic
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
    assert isinstance(static_definitions.resources["test-resource"], ResourceItem)
    assert len(static_definitions.resources_by_type["TestApiKey"]) == 1


def test_load_job_definition_unordered() -> None:
    # We can define objects in any order.
    # Its fine to define jobsdefinitions before the topic that they refer to.
    job_definition_str: str = """
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJobDefinition
metadata:
  name: test-job-definition
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

"""
    static_definitions = load_definitions_from_str(job_definition_str)

    assert len(static_definitions.inventories) == 1
    assert len(static_definitions.job_definitions) == 1
    assert len(static_definitions.topics) == 1


def test_load_job_definition_without_output() -> None:
    job_definition_str: str = """
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJobDefinition
metadata:
  name: test-job-definition
spec:
  minimalInterval: "@weekly"
  template:
    input:
      inventory: test-inventory

    pipeline:
      name: something.saturn.pipelines.aa.bb
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnInventory
metadata:
  name: test-inventory
spec:
  type: testtype
---

"""
    static_definitions = load_definitions_from_str(job_definition_str)

    assert len(static_definitions.job_definitions) == 1
    assert len(static_definitions.inventories) == 1


def test_load_job() -> None:
    job_definition_str: str = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnInventory
metadata:
  name: test-inventory
spec:
  type: testtype
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJob
metadata:
  name: test-job
spec:
  input:
    inventory: test-inventory

  pipeline:
    name: something.saturn.pipelines.aa.bb
    resources: {}
"""
    static_definitions = load_definitions_from_str(job_definition_str)

    assert len(static_definitions.jobs) == 1
    assert len(static_definitions.inventories) == 1
    assert len(static_definitions.job_definitions) == 0
    assert len(static_definitions.topics) == 0
    assert (
        static_definitions.jobs["test-job"].pipeline.info.name
        == "something.saturn.pipelines.aa.bb"
    )


def test_load_jobs() -> None:
    job_definition_str: str = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnInventory
metadata:
  name: test-inventory
spec:
  type: testtype
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJob
metadata:
  name: test-job
spec:
  input:
    inventory: test-inventory

  pipeline:
    name: something.saturn.pipelines.aa.bb
    resources: {}
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJob
metadata:
  name: test-job-2
spec:
  input:
    inventory: test-inventory

  pipeline:
    name: something.saturn.pipelines.aa.bb.cc
    resources: {}
"""
    static_definitions = load_definitions_from_str(job_definition_str)

    assert len(static_definitions.jobs) == 2
    assert len(static_definitions.inventories) == 1
    assert len(static_definitions.job_definitions) == 0
    assert len(static_definitions.topics) == 0
    assert (
        static_definitions.jobs["test-job"].pipeline.info.name
        == "something.saturn.pipelines.aa.bb"
    )
    assert (
        static_definitions.jobs["test-job-2"].pipeline.info.name
        == "something.saturn.pipelines.aa.bb.cc"
    )


def test_load_job_no_input() -> None:
    job_definition_str: str = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJob
metadata:
  name: test-job
spec:
  input: {}
  pipeline:
    name: something.saturn.pipelines.aa.bb
    resources: {}
"""
    with pytest.raises(
        Exception,
        match="JobInput must specify one of inventory or topic",
    ):
        load_definitions_from_str(job_definition_str)

    job_definition_str = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJob
metadata:
  name: test-job
spec:
  input:
    topic: aa
    inventory: bb
  pipeline:
    name: something.saturn.pipelines.aa.bb
    resources: {}
"""
    with pytest.raises(
        Exception,
        match="JobInput can't specify both inventory and topic",
    ):
        load_definitions_from_str(job_definition_str)


def test_already_defined() -> None:
    definitions: str = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnTopic
metadata:
  name: test-topic
spec:
  type: RabbitMQTopic
  options: {}
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnTopic
metadata:
  name: test-topic
spec:
  type: RabbitMQTopic
  options: {}
"""
    with pytest.raises(
        Exception,
        match="SaturnTopic/test-topic already exists",
    ):
        load_definitions_from_str(definitions)
