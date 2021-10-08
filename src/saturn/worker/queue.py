from typing import Generic
from typing import Optional
from typing import TypeVar

T = TypeVar("T")


class Queue(Generic[T]):
    def __init__(self) -> None:
        pass

    def get_nowait(self) -> Optional[T]:
        return None

    async def get(self) -> T:
        pass
