import dataclasses
from typing import Type

from .queues import Queue


@dataclasses.dataclass
class WorkItems:
    queues: list[Queue]
    tasks: list[object]

    @classmethod
    def empty(cls: Type["WorkItems"]) -> "WorkItems":
        return cls(queues=[], tasks=[])
