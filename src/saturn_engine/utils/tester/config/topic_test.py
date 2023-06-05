from typing import Optional

from pydantic import dataclasses

from saturn_engine.core import TopicMessage
from saturn_engine.utils.declarative_config import BaseObject


@dataclasses.dataclass
class TopicSelector:
    topic: str


@dataclasses.dataclass
class TopicTestSpec:
    selector: TopicSelector
    messages: list[TopicMessage]
    limit: Optional[int] = None
    skip: Optional[int] = None


@dataclasses.dataclass
class TopicTest(BaseObject):
    spec: TopicTestSpec
