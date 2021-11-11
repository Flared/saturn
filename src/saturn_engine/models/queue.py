from typing import Any
from typing import Optional

from sqlalchemy import Column
from sqlalchemy import Text
from sqlalchemy.orm import Mapped
from sqlalchemy.sql.sqltypes import DateTime

from saturn_engine.core.api import QueueItem
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import fromdict

from .base import Base
from .types import JSON


class Queue(Base):
    __tablename__ = "queues"

    name: Mapped[str] = Column(Text, primary_key=True)
    assigned_at = Column(DateTime(timezone=True))
    assigned_to = Column(Text)
    _spec: Mapped[dict[str, Any]] = Column("spec", JSON, nullable=False)  # type: ignore[assignment]  # noqa: B950
    job: Optional["Job"]
    _cached_spec: Optional[QueueItem] = None

    def __init__(self, *, name: str, spec: QueueItem) -> None:
        self.name = name
        self.spec = spec

    @property
    def spec(self) -> QueueItem:
        if self._cached_spec is None:
            self._cached_spec = fromdict(self._spec, QueueItem)
        return self._cached_spec

    @spec.setter
    def spec(self, value: QueueItem) -> None:
        self._cached_spec = value
        self._spec = asdict(value)


from .job import Job
