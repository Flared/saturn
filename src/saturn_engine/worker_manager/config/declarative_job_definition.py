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

    def to_core_object(
        self,
        static_definitions: StaticDefinitions,
    ) -> api.JobDefinition:
        return api.JobDefinition(
            name=self.metadata.name,
            template=self.spec.template.to_core_object(
                self.metadata.name, static_definitions
            ),
            minimal_interval=self.spec.minimalInterval,
        )
