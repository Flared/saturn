import dataclasses

from saturn_engine.core import api
from saturn_engine.utils.declarative_config import BaseObject
from saturn_engine.worker_manager.config.declarative_pipeline import PipelineInfo
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions


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
class JobSpec:
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
class Job(BaseObject):
    spec: JobSpec

    def to_core_object(
        self,
        static_definitions: StaticDefinitions,
    ) -> api.QueueItem:
        return api.QueueItem(
            name=self.metadata.name,
            input=self.spec.input.to_core_object(static_definitions),
            output={
                key: [t.to_core_object(static_definitions) for t in topics]
                for key, topics in self.spec.output.items()
            },
            pipeline=api.QueuePipeline(
                info=self.spec.pipeline.to_core_object(),
                args=dict(),
            ),
        )
