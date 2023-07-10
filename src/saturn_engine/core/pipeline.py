from typing import Any
from typing import Callable
from typing import Hashable
from typing import TypeVar
from typing import Union
from typing import cast

import dataclasses
import inspect

from saturn_engine.utils import inspect as extra_inspect

from .resource import Resource
from .topic import TopicMessage

T = TypeVar("T", bound=Hashable)


@dataclasses.dataclass
class PipelineInfo:
    name: str
    resources: dict[str, str]

    def into_pipeline(self) -> Callable:
        return cast(Callable, extra_inspect.import_name(self.name))

    @classmethod
    def from_pipeline(cls, pipeline: Callable) -> "PipelineInfo":
        name = extra_inspect.get_import_name(pipeline)
        try:
            signature = extra_inspect.signature(pipeline)
        except Exception as e:
            raise ValueError("Can't parse signature") from e
        resources = cls.get_resources(signature)
        return cls(name=name, resources=resources)

    @staticmethod
    def get_resources(signature: inspect.Signature) -> dict[str, str]:
        resources = {}
        for parameter in signature.parameters.values():
            if isinstance(parameter.annotation, type) and issubclass(
                parameter.annotation, Resource
            ):
                resources[parameter.name] = parameter.annotation._typename()
        return resources


@dataclasses.dataclass
class QueuePipeline:
    info: PipelineInfo
    args: dict[str, Any]


@dataclasses.dataclass
class PipelineOutput:
    channel: str
    message: TopicMessage


@dataclasses.dataclass
class ResourceUsed:
    type: str
    release_at: float

    @classmethod
    def from_resource(cls, resource: Resource, release_at: float) -> "ResourceUsed":
        return cls(type=resource._typename(), release_at=release_at)


PipelineResult = Union[ResourceUsed, PipelineOutput, TopicMessage]


@dataclasses.dataclass
class PipelineResults:
    outputs: list[PipelineOutput]
    resources: list[ResourceUsed]
