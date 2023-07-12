import dataclasses

from saturn_engine.core.pipeline import PipelineEvent


@dataclasses.dataclass
class CursorStateUpdated(PipelineEvent):
    state: dict


class CursorState:
    pass
