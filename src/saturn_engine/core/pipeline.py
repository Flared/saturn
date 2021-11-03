import dataclasses
import inspect
from typing import Any
from typing import Callable
from typing import cast

from saturn_engine.utils import inspect as extra_inspect
from saturn_engine.utils.options import schema_for

from .resource import Resource


@dataclasses.dataclass
class PipelineInfo:
    name: str
    resources: dict[str, str]

    def into_pipeline(self) -> Callable:
        return cast(Callable, extra_inspect.import_name(self.name))

    @classmethod
    def from_pipeline(cls, pipeline: Callable) -> "PipelineInfo":
        name = extra_inspect.get_import_names(pipeline)
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
    def instancify_args(args: dict[str, object], pipeline: Callable) -> None:
        signature = extra_inspect.signature(pipeline)
        for parameter in signature.parameters.values():
            if parameter.name not in args:
                continue
            data = args[parameter.name]
            if not isinstance(data, dict):
                continue

            if dataclasses.is_dataclass(parameter.annotation):
                schema = schema_for(parameter.annotation)
                args[parameter.name] = schema.load(data)


@dataclasses.dataclass
class QueuePipeline:
    info: PipelineInfo
    args: dict[str, Any]
