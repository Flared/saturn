from typing import Any
from typing import Optional

from . import JobStore


class MemoryJobStore(JobStore):
    after: Optional[str]

    def __init__(self, **kwargs: Any) -> None:
        self.after = None

    async def load_cursor(self) -> Optional[str]:
        return self.after

    async def save_cursor(self, *, after: str) -> None:
        self.after = after
