import dataclasses
import uuid

from .pipeline import PipelineInfo


@dataclasses.dataclass
class TopicMessage:
    args: dict[str, object]
    id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))

    def extend(self, args: dict[str, object]) -> "TopicMessage":
        return self.__class__(id=self.id, args=args | self.args)


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


@dataclasses.dataclass
class PipelineOutput:
    channel: str
    message: TopicMessage
