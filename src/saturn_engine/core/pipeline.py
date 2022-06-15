import typing
from typing import Any
from typing import Callable
from typing import Hashable
from typing import Type
from typing import TypeVar
from typing import Union
from typing import cast

import dataclasses
import inspect

from saturn_engine.utils import inspect as extra_inspect
from saturn_engine.utils.options import schema_for

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
            if issubclass(parameter.annotation, Resource):
                resources[parameter.name] = parameter.annotation._typename()
        return resources

    @staticmethod
    def _instancify_dataclass(
        data: dict[str, object],
        target_type: Type[T],
    ) -> Union[T, dict[str, object]]:
        if dataclasses.is_dataclass(target_type):
            schema = schema_for(target_type)
            return schema.load(data)
        return data

    @classmethod
    def instancify_args(cls, args: dict[str, object], pipeline: Callable) -> None:
        signature = extra_inspect.signature(pipeline)
        for parameter in signature.parameters.values():
            if parameter.name not in args:
                continue
            data = args[parameter.name]
            if not isinstance(data, (dict, list)):
                continue

            target_type = parameter.annotation

            # Support Optional[dataclass], which is very common.
            # This does not support Union of two dataclasses yet.
            if typing.get_origin(target_type) is typing.Union:
                target_args = [
                    arg
                    for arg in typing.get_args(target_type)
                    if not isinstance(None, arg)
                ]
                if len(target_args) == 1:
                    target_type = target_args[0]

            if isinstance(data, dict):
                args[parameter.name] = cls._instancify_dataclass(
                    data=data,
                    target_type=target_type,
                )
            else:
                target_args = list(typing.get_args(target_type))
                if len(target_args) == 1:
                    args[parameter.name] = [
                        cls._instancify_dataclass(
                            data=item,
                            target_type=target_args[0],
                        )
                        for item in data
                    ]


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
