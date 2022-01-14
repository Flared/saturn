import dataclasses

from saturn_engine.core import api
from saturn_engine.utils.declarative_config import BaseObject

from .declarative_pipeline import PipelineInfo
from .static_definitions import StaticDefinitions


@dataclasses.dataclass
class JobInput:
    inventory: str

    def to_core_object(
        self,
        static_definitions: StaticDefinitions,
    ) -> api.InventoryItem:
        return static_definitions.inventories[self.inventory]


@dataclasses.dataclass
class JobOutput:
    topic: str

    def to_core_object(
        self,
        static_definitions: StaticDefinitions,
    ) -> api.TopicItem:
        return static_definitions.topics[self.topic]


@dataclasses.dataclass
class JobDefinitionTemplate:
    name: str
    input: JobInput
    pipeline: PipelineInfo
    output: dict[str, list[JobOutput]] = dataclasses.field(default_factory=dict)

    def to_core_object(
        self,
        static_definitions: StaticDefinitions,
    ) -> api.QueueItem:
        return api.QueueItem(
            name=self.name,
            input=self.input.to_core_object(static_definitions),
            output={
                key: [t.to_core_object(static_definitions) for t in topics]
                for key, topics in self.output.items()
            },
            pipeline=api.QueuePipeline(
                info=self.pipeline.to_core_object(),
                args=dict(),
            ),
        )


@dataclasses.dataclass
class JobDefinitionSpec:
    template: JobDefinitionTemplate
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
                static_definitions,
            ),
            minimal_interval=self.spec.minimalInterval,
        )
