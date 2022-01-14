from typing import Any

import dataclasses

from saturn_engine.core import api


@dataclasses.dataclass
class PipelineInfo:
    name: str
    resources: dict[str, str] = dataclasses.field(default_factory=dict)

    def to_core_object(self) -> api.PipelineInfo:
        return api.PipelineInfo(
            name=self.name,
            resources=self.resources,
        )


@dataclasses.dataclass
class PipelineSpec:
    info: PipelineInfo
    args: dict[str, Any] = dataclasses.field(default_factory=dict)

    def to_core_object(self) -> api.QueuePipeline:
        return api.QueuePipeline(
            info=self.info.to_core_object(),
            args=self.args,
        )
