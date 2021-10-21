from typing import Any
from typing import Optional

from . import JobStore


class MemoryJobStore(JobStore):
    def __init__(self, **kwargs: Any) -> None:
        self.after: Optional[int] = None

    async def load_cursor(self) -> Optional[int]:
        return self.after

    async def save_cursor(self, *, after: int) -> None:
        self.after = after
