import typing as t

import dataclasses
from dataclasses import field

from saturn_engine.core import JobId
from saturn_engine.core import api
from saturn_engine.utils.declarative_config import BaseObject
from saturn_engine.worker_manager.config.declarative_pipeline import PipelineInfo
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions

DEFAULT_INPUT_NAME: t.Final[str] = "default"
JOB_KIND: t.Final[str] = "SaturnJob"


@dataclasses.dataclass
class JobInput:
    inventory: t.Optional[str] = None
    topic: t.Optional[str] = None

    def __post_init__(self) -> None:
        if not self.inventory and not self.topic:
            raise Exception("JobInput must specify one of inventory or topic")
        if self.inventory and self.topic:
            raise Exception("JobInput can't specify both inventory and topic")

    def to_core_object(
        self,
        static_definitions: StaticDefinitions,
    ) -> t.Union[api.ComponentDefinition, api.ComponentDefinition]:
        if self.inventory:
            return static_definitions.inventories[self.inventory]
        elif self.topic:
            return static_definitions.topics[self.topic]
        else:
            raise Exception("JobInput has no job or topic")


@dataclasses.dataclass
class JobOutputTopic:
    topic: str

    def to_core_object(
        self,
        static_definitions: StaticDefinitions,
    ) -> api.OutputDefinition:
        return static_definitions.topics[self.topic]


class JobOutputErrorHandler(api.ErrorHandler):
    def to_core_object(
        self,
        static_definitions: StaticDefinitions,
    ) -> api.OutputDefinition:
        return self


JobOutput = t.Union[JobOutputTopic, JobOutputErrorHandler]


@dataclasses.dataclass
class JobSpec:
    pipeline: PipelineInfo
    input: t.Optional[JobInput] = None
    inputs: dict[str, JobInput] = field(default_factory=dict)
    output: dict[str, list[JobOutput]] = field(default_factory=dict)
    config: dict[str, t.Any] = field(default_factory=dict)
    executor: str = "default"

    def to_core_objects(
        self,
        name: str,
        labels: dict[str, str],
        static_definitions: StaticDefinitions,
    ) -> t.Iterator[api.QueueItem]:
        inputs = {k: v for k, v in self.inputs.items()}
        if self.input:
            inputs[DEFAULT_INPUT_NAME] = self.input

        if not inputs:
            raise Exception("JobSpec has no input")

        for job_input_name, job_input in inputs.items():
            queue_item_name = name
            if job_input_name != DEFAULT_INPUT_NAME:
                queue_item_name = f"{name}-{job_input_name}"

            yield api.QueueItem(
                name=JobId(queue_item_name),
                input=job_input.to_core_object(static_definitions),
                output={
                    key: [o.to_core_object(static_definitions) for o in outputs]
                    for key, outputs in self.output.items()
                },
                pipeline=api.QueuePipeline(
                    info=self.pipeline.to_core_object(),
                    args=dict(),
                ),
                labels=dict(
                    {
                        "internal.job-definition-name": name,
                    },
                    **labels,
                ),
                config=self.config,
                executor=self.executor,
            )


@dataclasses.dataclass(kw_only=True)
class Job(BaseObject):
    spec: JobSpec
    kind: str = JOB_KIND

    def to_core_objects(
        self,
        static_definitions: StaticDefinitions,
    ) -> t.Iterator[api.QueueItem]:
        yield from self.spec.to_core_objects(
            self.metadata.name,
            self.metadata.labels,
            static_definitions,
        )
