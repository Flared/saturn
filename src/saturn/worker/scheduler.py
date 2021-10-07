from typing import Generic
from typing import Iterator
from typing import TypeVar

T = TypeVar("T")


class Scheduler(Generic[T]):
    def __init__(self) -> None:
        pass

    def iter_ready(self) -> Iterator[T]:
        return iter([])

    def set_processing(self, queue: T) -> None:
        pass

    def add(self, queue: T) -> None:
        pass

    def remove(self, queue: T) -> None:
        pass

    async def wait_ready(self) -> None:
        pass
