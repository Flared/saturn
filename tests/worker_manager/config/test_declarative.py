import os

import pytest
from flask.testing import FlaskClient
from sqlalchemy.orm import Session

from saturn_engine.core.api import ComponentDefinition
from saturn_engine.core.api import JobDefinition
from saturn_engine.core.api import ResourceItem
from saturn_engine.stores import topologies_store
from saturn_engine.utils.declarative_config import BaseObject
from saturn_engine.utils.declarative_config import ObjectMetadata
from saturn_engine.utils.declarative_config import load_uncompiled_objects_from_str
from saturn_engine.worker_manager.config.declarative import compile_static_definitions
from saturn_engine.worker_manager.config.declarative import filter_with_jobs_selector
from saturn_engine.worker_manager.config.declarative import load_definitions_from_paths
from saturn_engine.worker_manager.config.declarative import load_definitions_from_str
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions


def test_load_definitions_from_paths_simple() -> None:
    test_dir = os.path.join(
        os.path.dirname(__file__),
        "testdata",
        "test_declarative_simple",
    )
    static_definitions = load_definitions_from_paths([test_dir])
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
  rate_limit:
    rate_limits:
    - 10 per hour
    strategy: moving-window
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

    executor: default
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

    assert isinstance(
        static_definitions.inventories["test-inventory"], ComponentDefinition
    )
    assert isinstance(static_definitions.topics["test-topic"], ComponentDefinition)
    assert isinstance(
        static_definitions.job_definitions["test-job-definition"], JobDefinition
    )
    assert isinstance(static_definitions.resources["test-resource"], ResourceItem)
    assert len(static_definitions.resources_by_type["TestApiKey"]) == 1
    assert static_definitions.resources["test-resource"].rate_limit
    assert (
        static_definitions.resources["test-resource"].rate_limit.strategy
        == "moving-window"
    )
    assert (
        len(static_definitions.resources["test-resource"].rate_limit.rate_limits) == 1
    )


def test_load_job_definition_unordered() -> None:
    # We can define objects in any order.
    # Its fine to define jobsdefinitions before the topic that they refer to.
    job_definition_str: str = """
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJobDefinition
metadata:
  name: test-job-definition
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


def test_load_job_definition_multiple_inputs() -> None:
    job_definition_str: str = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnInventory
metadata:
  name: test-inventory
spec:
  type: testtype
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
kind: SaturnJobDefinition
metadata:
  name: test-job-definition
  labels:
    owner: team-saturn
spec:
  minimalInterval: "@weekly"
  template:
      inputs:
        default:
            inventory: test-inventory
        with-topic-input:
            topic: test-topic
      pipeline:
        name: something.saturn.pipelines.aa.bb
        resources: {}
"""
    static_definitions = load_definitions_from_str(job_definition_str)

    assert len(static_definitions.jobs) == 0
    assert len(static_definitions.inventories) == 1
    assert len(static_definitions.job_definitions) == 2
    assert len(static_definitions.topics) == 1
    assert (
        static_definitions.job_definitions["test-job-definition"].template.input.name
        == "test-inventory"
    )
    assert (
        static_definitions.job_definitions[
            "test-job-definition-with-topic-input"
        ].template.input.name
        == "test-topic"
    )


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
  labels:
    owner: team-saturn
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
  labels:
    owner: team-saturn
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
  labels:
    owner: team-saturn
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


def test_load_jobs_multiple_inputs() -> None:
    job_definition_str: str = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnInventory
metadata:
  name: test-inventory
spec:
  type: testtype
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
kind: SaturnJob
metadata:
  name: test-job
  labels:
    owner: team-saturn
spec:
  inputs:
    default:
        inventory: test-inventory
    with-topic-input:
        topic: test-topic
  pipeline:
    name: something.saturn.pipelines.aa.bb
    resources: {}
"""
    static_definitions = load_definitions_from_str(job_definition_str)

    assert len(static_definitions.jobs) == 2
    assert len(static_definitions.inventories) == 1
    assert len(static_definitions.job_definitions) == 0
    assert len(static_definitions.topics) == 1
    assert static_definitions.jobs["test-job"].input.name == "test-inventory"
    assert (
        static_definitions.jobs["test-job-with-topic-input"].input.name == "test-topic"
    )


def test_load_job_no_input() -> None:
    job_definition_str: str = """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJob
metadata:
  name: test-job
  labels:
    owner: team-saturn
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
  labels:
    owner: team-saturn
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


def test_filter_with_jobs_selector() -> None:
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
  name: test-1-job
  labels:
    owner: team-saturn
spec:
  input:
    inventory: test-inventory
  pipeline:
    name: something.saturn.pipelines.aa.bb
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJob
metadata:
  name: test-2-job
  labels:
    owner: team-saturn
spec:
  input:
    inventory: test-inventory
  pipeline:
    name: something.saturn.pipelines.aa.bb.cc
---
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJobDefinition
metadata:
  name: test-1-job-definition
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
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnJobDefinition
metadata:
  name: test-2-job-definition
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

    def names(definitions: StaticDefinitions) -> dict[str, set[str]]:
        return {
            "jobs": set(definitions.jobs.keys()),
            "job_definitions": set(definitions.job_definitions.keys()),
        }

    static_definitions = load_definitions_from_str(job_definition_str)
    assert names(
        filter_with_jobs_selector(definitions=static_definitions, selector="test-1-")
    ) == {"jobs": {"test-1-job"}, "job_definitions": {"test-1-job-definition"}}

    assert names(
        filter_with_jobs_selector(
            definitions=static_definitions, selector="test-.-job-definition"
        )
    ) == {
        "jobs": set(),
        "job_definitions": {"test-1-job-definition", "test-2-job-definition"},
    }

    assert names(
        filter_with_jobs_selector(definitions=static_definitions, selector="foobar")
    ) == {
        "jobs": set(),
        "job_definitions": set(),
    }

    assert names(
        filter_with_jobs_selector(definitions=static_definitions, selector=".*")
    ) == {
        "jobs": {"test-1-job", "test-2-job"},
        "job_definitions": {"test-1-job-definition", "test-2-job-definition"},
    }


def test_load_executor() -> None:
    executor_definition_str = """
    apiVersion: saturn.flared.io/v1alpha1
    kind: SaturnExecutor
    metadata:
      name: test-executor
    spec:
      type: ProcessExecutor
      options:
        pool_size: 2
    """

    static_definitions = load_definitions_from_str(executor_definition_str)

    assert len(static_definitions.executors) == 1
    assert static_definitions.executors["test-executor"].options["pool_size"] == 2


def test_resource_concurrency() -> None:
    concurrency_definition_str = """
    apiVersion: saturn.flared.io/v1alpha1
    kind: SaturnResource
    metadata:
      name: test-resource
    spec:
      type: TestApiKey
      data:
        key: "qwe"
      default_delay: 10
      concurrency: 5
    """
    static_definitions = load_definitions_from_str(concurrency_definition_str)
    assert len(static_definitions.resources) == 5
    for i in range(1, 6):
        assert f"test-resource-{i}" in static_definitions.resources


def test_resources_provider() -> None:
    resources_provider_str = """
    apiVersion: saturn.flared.io/v1alpha1
    kind: SaturnResourcesProvider
    metadata:
      name: test-resource-provider
    spec:
      type: TestApiKeyProvider
      resource_type: TestApiKey
      options:
        url: "http://qwe.com"
    """
    static_definitions = load_definitions_from_str(resources_provider_str)
    assert len(static_definitions.resources_providers) == 1
    assert len(static_definitions.resources_by_type["TestApiKey"]) == 1


def test_dynamic_definition() -> None:
    resources_provider_str = """
    apiVersion: saturn.flared.io/v1alpha1
    kind: SaturnDynamicTopology
    metadata:
      name: test-dynamic-topology
    spec:
      module: tests.worker_manager.config.dynamic_definition.build
    """
    static_definitions = load_definitions_from_str(resources_provider_str)
    assert "test-inventory" in static_definitions.inventories
    assert static_definitions.inventories["test-inventory"].name == "test-inventory"


def test_compile_static_definitions_with_patches(
    client: FlaskClient, session: Session
) -> None:
    concurrency_definition_str = """
    apiVersion: saturn.flared.io/v1alpha1
    kind: SaturnResource
    metadata:
      name: test-resource
      labels:
        owner: team-saturn
    spec:
      type: TestApiKey
      data:
        key: "qwe"
      default_delay: 10
      concurrency: 2
    """

    uncompiled_objects = load_uncompiled_objects_from_str(concurrency_definition_str)

    compileed_static_definitions_without_patch = compile_static_definitions(
        uncompiled_objects=uncompiled_objects
    )

    assert compileed_static_definitions_without_patch.resources == {
        "test-resource-1": ResourceItem(
            name="test-resource-1",
            type="TestApiKey",
            data={"key": "qwe"},
            default_delay=10.0,
            rate_limit=None,
        ),
        "test-resource-2": ResourceItem(
            name="test-resource-2",
            type="TestApiKey",
            data={"key": "qwe"},
            default_delay=10.0,
            rate_limit=None,
        ),
    }

    # Now we create a patch to change the resource concurrency
    patch = topologies_store.patch(
        session=session,
        patch=BaseObject(
            kind="SaturnResource",
            apiVersion="saturn.flared.io/v1alpha1",
            metadata=ObjectMetadata(name="test-resource"),
            spec={"concurrency": 1},
        ),
    )

    compileed_static_definitions_without_patch = compile_static_definitions(
        uncompiled_objects=uncompiled_objects, patches=[patch]
    )

    assert compileed_static_definitions_without_patch.resources == {
        "test-resource": ResourceItem(
            name="test-resource",
            type="TestApiKey",
            data={"key": "qwe"},
            default_delay=10.0,
            rate_limit=None,
        )
    }
