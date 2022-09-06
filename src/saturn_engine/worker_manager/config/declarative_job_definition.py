import typing as t

import dataclasses

from saturn_engine.core import api
from saturn_engine.utils.declarative_config import BaseObject
from saturn_engine.worker_manager.config.declarative_job import JobSpec

from .static_definitions import StaticDefinitions


@dataclasses.dataclass
class JobDefinitionSpec:
    template: JobSpec
    minimalInterval: str


@dataclasses.dataclass
class JobDefinition(BaseObject):
    spec: JobDefinitionSpec

    def to_core_objects(
        self,
        static_definitions: StaticDefinitions,
    ) -> t.Iterator[api.JobDefinition]:
        for template in self.spec.template.to_core_objects(
            self.metadata.name, static_definitions
        ):
            yield api.JobDefinition(
                name=template.name,
                template=template,
                minimal_interval=self.spec.minimalInterval,
            )
