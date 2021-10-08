from typing import Generic
from typing import Iterator
from typing import TypeVar

T = TypeVar("T")


class Scheduler(Generic[T]):
    queues: list[T]

    def __init__(self) -> None:
        self.queues = []

    def iter_ready(self) -> Iterator[T]:
        return iter(self.queues)

    def set_processing(self, queue: T) -> None:
        pass

    def add(self, queue: T) -> None:
        self.queues.append(queue)

    def remove(self, queue: T) -> None:
        self.queues.remove(queue)

    async def wait_ready(self) -> None:
        pass
