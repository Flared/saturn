from typing import Optional

from datetime import datetime

import sqlalchemy as sa
from sqlalchemy import CheckConstraint
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship

import saturn_engine.models.queue as queue_model
from saturn_engine.core import Cursor
from saturn_engine.core import JobId
from saturn_engine.core.api import JobItem
from saturn_engine.models.compat import mapped_column
from saturn_engine.utils import utcnow

from .base import Base
from .types import UTCDateTime


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(
            "(error IS NULL) OR (error IS NOT NULL AND completed_at IS NOT NULL)",
            name="jobs_error_must_also_be_completed",
        ),
    )

    name: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    cursor: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(UTCDateTime, nullable=True)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime, nullable=False)
    queue_name: Mapped[str] = mapped_column(
        sa.Text, ForeignKey("queues.name"), nullable=False
    )
    error: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
    queue: Mapped[queue_model.Queue] = relationship(
        lambda: queue_model.Queue,
        uselist=False,
        back_populates="job",
    )
    job_definition_name: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)

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
