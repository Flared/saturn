import typing as t

import dataclasses

from saturn_engine.core import PipelineInfo
from saturn_engine.core import TopicMessage
from saturn_engine.utils.inspect import dataclass_from_params
from saturn_engine.utils.inspect import get_import_name
from saturn_engine.utils.inspect import import_name
from saturn_engine.utils.options import fromdict


@dataclasses.dataclass
class PipelineMessage:
    info: PipelineInfo
    message: TopicMessage
    meta_args: dict[str, t.Any] = dataclasses.field(default_factory=dict)

    @property
    def id(self) -> str:
        return self.message.id

    @property
    def missing_resources(self) -> set[str]:
        return {
            typ
            for name, typ in self.info.resources.items()
            if name not in self.message.args
        }

    def update_with_resources(self, resources: dict[str, dict]) -> None:
        for name, typ in self.info.resources.items():
            if name not in self.message.args:
                self.message.args[name] = resources[typ]

    @property
    def resource_names(self) -> dict[str, t.Optional[str]]:
        return {
            typ: self.message.args[name].get("name")  # type: ignore[union-attr]
            for name, typ in self.info.resources.items()
            if isinstance(self.message.args.get(name), dict)
        }

    def set_meta_arg(self, *, meta_type: t.Type, value: t.Any) -> None:
        self.meta_args[get_import_name(meta_type)] = value

    def execute(self) -> object:
        pipeline = self.info.into_pipeline()
        pipeline_args_def = dataclass_from_params(pipeline)
        args = self.message.args

        for meta_name, value in self.meta_args.items():
            meta_type = import_name(meta_name)
            arg = pipeline_args_def.find_by_type(meta_type)
            if arg:
                args[arg] = value

        pipeline_args = fromdict(args, pipeline_args_def)
        return pipeline_args.call(kwargs=args)

    def as_remote(self) -> "PipelineMessage":
        return dataclasses.replace(
            self, message=dataclasses.replace(self.message, config={})
        )
