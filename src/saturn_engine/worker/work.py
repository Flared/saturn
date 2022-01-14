from typing import Type

import dataclasses

from .queue import ExecutableQueue
from .scheduler import Schedulable


class SchedulableQueue(Schedulable):
    queue: ExecutableQueue

    def __init__(self, queue: ExecutableQueue) -> None:
        super().__init__(iterable=queue.run())
        self.queue = queue


@dataclasses.dataclass
class WorkItems:
    queues: list[SchedulableQueue]
    tasks: list[object]

    @classmethod
    def empty(cls: Type["WorkItems"]) -> "WorkItems":
        return cls(queues=[], tasks=[])
