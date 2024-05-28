import dataclasses

from saturn_engine.core import Cursor
from saturn_engine.core.pipeline import PipelineEvent


@dataclasses.dataclass
class CursorStateUpdated(PipelineEvent):
    state: dict
    cursor: Cursor | None = None


class CursorState:
    pass
