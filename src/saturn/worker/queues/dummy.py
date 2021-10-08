from typing import Optional

from . import Queue


class DummyQueue(Queue[str]):
    def __init__(self) -> None:
        pass

    def get_nowait(self) -> Optional[str]:
        return "hello"

    async def get(self) -> str:
        return "hello"
