import pytest

from saturn_engine.models import Queue
from saturn_engine.worker_manager.config.declarative import load_definitions_from_str

static_job_definition: str = """
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


def test_join_definitions_should_raise_exception_on_undefined_job_object() -> None:
    with pytest.raises(NotImplementedError):
        static_definitions = load_definitions_from_str(static_job_definition)
        Queue(name="test").join_definitions(static_definitions)
