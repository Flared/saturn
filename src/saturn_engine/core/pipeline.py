import dataclasses
import inspect
from typing import Callable
from typing import cast

from saturn_engine.utils import inspect as extra_inspect

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
            signature = inspect.signature(pipeline)
            signature = extra_inspect.eval_annotations(pipeline, signature)
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


@dataclasses.dataclass
class QueuePipeline:
    info: PipelineInfo
    args: dict[str, object]
