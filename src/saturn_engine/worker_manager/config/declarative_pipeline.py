import dataclasses
from dataclasses import field

from saturn_engine.core import api


@dataclasses.dataclass
class PipelineInfo:
    name: str
    resources: dict[str, str] = field(default_factory=dict)

    def to_core_object(self) -> api.PipelineInfo:
        return api.PipelineInfo(
            name=self.name,
            resources=self.resources,
        )
