from collections.abc import Iterable
from typing import Optional

from saturn_engine.core import Message


class Inventory:
    def __init__(self) -> None:
        pass

    async def next_batch(self, after: Optional[str] = None) -> Iterable[Message]:
        return []
