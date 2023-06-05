from typing import Any

from dataclasses import field

from pydantic import dataclasses

from saturn_engine.core import api
from saturn_engine.utils.declarative_config import BaseObject


@dataclasses.dataclass
class ExecutorSpec:
    type: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclasses.dataclass
class Executor(BaseObject):
    spec: ExecutorSpec

    def to_core_object(self) -> api.ComponentDefinition:
        return api.ComponentDefinition(
            name=self.metadata.name,
            type=self.spec.type,
            options=self.spec.options,
        )
