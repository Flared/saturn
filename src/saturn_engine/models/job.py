from typing import Optional

from datetime import datetime

from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import backref
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import Text
from sqlalchemy.types import JSON

from saturn_engine.core import Cursor
from saturn_engine.core import JobId
from saturn_engine.core.api import JobItem
from saturn_engine.utils import utcnow

from .base import Base
from .types import UTCDateTime


class JobCursorState(Base):
    __tablename__ = "job_cursor_states"
    job_definition_name: Mapped[str] = Column(Text, primary_key=True)
    cursor: Mapped[str] = Column(Text, primary_key=True)
    state: Mapped[dict] = Column(JSON, nullable=False)


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(
            "(error IS NULL) OR (error IS NOT NULL AND completed_at IS NOT NULL)",
            name="jobs_error_must_also_be_completed",
        ),
    )

    name: Mapped[str] = Column(Text, primary_key=True)
    cursor = Column(Text, nullable=True)
    completed_at: Mapped[Optional[datetime]] = Column(UTCDateTime, nullable=True)  # type: ignore[assignment]  # noqa: B950
    started_at: Mapped[datetime] = Column(UTCDateTime, nullable=False)  # type: ignore[assignment]  # noqa: B950
    queue_name: Mapped[str] = Column(Text, ForeignKey("queues.name"), nullable=False)
    error = Column(Text, nullable=True)
    queue: "Queue" = relationship(
        "Queue",
        uselist=False,
        backref=backref("job", uselist=False),
    )
    job_definition_name: Mapped[Optional[str]] = Column(Text, nullable=True)

    def __init__(
        self,
        *,
        name: str,
        queue_name: str,
        job_definition_name: Optional[str] = None,
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
        queue_args = {}
        if self.queue:
            queue_args = {
                "enabled": self.queue.enabled,
                "assigned_at": self.queue.assigned_at,
                "assigned_to": self.queue.assigned_to,
            }
        return JobItem(
            name=JobId(self.name),
            completed_at=self.completed_at,
            started_at=self.started_at,
            cursor=Cursor(self.cursor) if self.cursor else None,
            error=self.error,
            **queue_args,  # type: ignore[arg-type]
        )


from .queue import Queue
