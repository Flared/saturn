import dataclasses

from saturn_engine.core import PipelineInfo
from saturn_engine.core import TopicMessage


@dataclasses.dataclass
class PipelineMessage:
    info: PipelineInfo
    message: TopicMessage

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

    def execute(self) -> object:
        pipeline = self.info.into_pipeline()
        PipelineInfo.instancify_args(self.message.args, pipeline=pipeline)

        return pipeline(**self.message.args)
