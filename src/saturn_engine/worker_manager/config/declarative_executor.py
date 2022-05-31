from typing import Any

import dataclasses

from saturn_engine.core import api
from saturn_engine.utils.declarative_config import BaseObject


@dataclasses.dataclass
class ExecutorSpec:
    type: str
    options: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class Executor(BaseObject):
    spec: ExecutorSpec

    def to_core_object(self) -> api.Executor:
        return api.Executor(
            name=self.metadata.name,
            type=self.spec.type,
            options=self.spec.options,
        )
