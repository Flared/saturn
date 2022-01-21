from typing import Any
from typing import Optional

from . import JobStore


class MemoryJobStore(JobStore):
    def __init__(self, **kwargs: Any) -> None:
        self.after: Optional[str] = None
        self.completed = False
        self.error: Optional[Exception] = None

    async def load_cursor(self) -> Optional[str]:
        return self.after

    async def save_cursor(self, *, after: str) -> None:
        self.after = after

    async def set_completed(self) -> None:
        self.completed = True

    async def set_failed(self, error: Exception) -> None:
        self.error = error
