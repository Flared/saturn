from typing import Generic
from typing import TypeVar

T = TypeVar("T")


class Queue(Generic[T]):
    def __init__(self) -> None:
        pass

    async def get(self) -> T:
        pass
