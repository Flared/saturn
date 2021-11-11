import dataclasses
from typing import Any

from saturn_engine.core import api

from .declarative_base import BaseObject


@dataclasses.dataclass
class TopicSpec:
    type: str
    options: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class TopicItem(BaseObject):
    spec: TopicSpec

    def to_core_object(self) -> api.TopicItem:
        return api.TopicItem(
            name=self.metadata.name,
            type=self.spec.type,
            options=self.spec.options,
        )
