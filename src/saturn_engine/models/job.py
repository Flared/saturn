from typing import Optional

from datetime import datetime

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import backref
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import Text

from saturn_engine.core.api import JobItem
from saturn_engine.utils import utcnow

from .base import Base
from .types import UTCDateTime


class Job(Base):
    __tablename__ = "jobs"

    name: Mapped[str] = Column(Text, primary_key=True)
    cursor = Column(Text, nullable=True)
    completed_at: Mapped[Optional[datetime]] = Column(UTCDateTime, nullable=True)  # type: ignore[assignment]  # noqa: B950
    started_at: Mapped[datetime] = Column(UTCDateTime, nullable=False)  # type: ignore[assignment]  # noqa: B950
    queue_name = Column(Text, ForeignKey("queues.name"), nullable=False)
    error = Column(Text, nullable=True)
    queue: "Queue" = relationship(
        "Queue",
        uselist=False,
        backref=backref("job", uselist=False),
    )
    job_definition_name: Mapped[str] = Column(Text, nullable=False)

    def __init__(
        self,
        *,
        name: str,
        queue_name: str,
        job_definition_name: str,
        completed_at: Optional[datetime] = None,
        started_at: Optional[datetime] = None,
        error: Optional[str] = None,
    ) -> None:
        self.name = name
        self.queue_name = queue_name
        self.job_definition_name = job_definition_name
        self.completed_at = completed_at
        self.started_at = started_at or utcnow()
        self.error = error

    def as_core_item(self) -> JobItem:
        return JobItem(
            name=self.name,
            completed_at=self.completed_at,
            started_at=self.started_at,
            cursor=self.cursor,
            error=self.error,
        )


from .queue import Queue
