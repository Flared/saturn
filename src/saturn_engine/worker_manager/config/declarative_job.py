from typing import Optional
from typing import Union

import dataclasses

from saturn_engine.core import api
from saturn_engine.utils.declarative_config import BaseObject
from saturn_engine.worker_manager.config.declarative_pipeline import PipelineInfo
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions


@dataclasses.dataclass
class JobInput:
    inventory: Optional[str]
    topic: Optional[str]

    def __post_init__(self) -> None:
        if not self.inventory and not self.topic:
            raise Exception("JobInput must specify one of inventory or topic")
        if self.inventory and self.topic:
            raise Exception("JobInput can't specify both inventory and topic")

    def to_core_object(
        self,
        static_definitions: StaticDefinitions,
    ) -> Union[api.InventoryItem, api.TopicItem]:
        if self.inventory:
            return static_definitions.inventories[self.inventory]
        elif self.topic:
            return static_definitions.topics[self.topic]
        else:
            raise Exception("JobInput has no job or topic")


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
    input: JobInput
    pipeline: PipelineInfo
    output: dict[str, list[JobOutput]] = dataclasses.field(default_factory=dict)
    executor: str = "default"

    def to_core_object(
        self,
        name: str,
        static_definitions: StaticDefinitions,
    ) -> api.QueueItem:
        return api.QueueItem(
            name=name,
            input=self.input.to_core_object(static_definitions),
            output={
                key: [t.to_core_object(static_definitions) for t in topics]
                for key, topics in self.output.items()
            },
            pipeline=api.QueuePipeline(
                info=self.pipeline.to_core_object(),
                args=dict(),
            ),
            executor=self.executor,
        )


@dataclasses.dataclass
class Job(BaseObject):
    spec: JobSpec

    def to_core_object(
        self,
        static_definitions: StaticDefinitions,
    ) -> api.QueueItem:
        return self.spec.to_core_object(self.metadata.name, static_definitions)
