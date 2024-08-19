import typing as t

import dataclasses
from dataclasses import field

from saturn_engine.core import api
from saturn_engine.utils.declarative_config import BaseObject

TOPIC_ITEM_KIND: t.Final[str] = "SaturnTopic"


@dataclasses.dataclass
class TopicSpec:
    type: str
    options: dict[str, t.Any] = field(default_factory=dict)


@dataclasses.dataclass(kw_only=True)
class TopicItem(BaseObject):
    spec: TopicSpec
    kind: str = TOPIC_ITEM_KIND

    def to_core_object(self) -> api.ComponentDefinition:
        return api.ComponentDefinition(
            name=self.metadata.name,
            type=self.spec.type,
            options=self.spec.options,
        )
