from typing import Optional

import dataclasses

from sqlalchemy import Index
from sqlalchemy import text
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import DateTime

import saturn_engine.models.job as job_model
from saturn_engine.core import Cursor
from saturn_engine.core.api import QueueItemState
from saturn_engine.core.api import QueueItemWithState
from saturn_engine.core.types import JobId
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions

from .base import Base


class Queue(Base):
    __allow_unmapped__ = True

    __tablename__ = "queues"
    __table_args__ = (
        Index(
            "queues_enabled_assigned_at",
            text("assigned_at"),
            postgresql_where="enabled",
        ),
    )

    name: Mapped[str] = mapped_column(primary_key=True)
    assigned_at = mapped_column(DateTime(timezone=True))
    assigned_to: Mapped[Optional[str]] = mapped_column()
    job = relationship(lambda: job_model.Job, uselist=False, back_populates="queue")
    _queue_item: Optional[QueueItemWithState] = None
    enabled: Mapped[bool] = mapped_column(default=True, nullable=False)

    @property
    def queue_item(self) -> QueueItemWithState:
        if self._queue_item is None:
            raise ValueError("Must .join_definitions() first")
        return self._queue_item

    def join_definitions(self, static_definitions: StaticDefinitions) -> None:
        if self.job:
            state = QueueItemState(
                cursor=Cursor(self.job.cursor) if self.job.cursor else None,
                started_at=self.job.started_at,
            )
            if self.job.job_definition_name is not None:
                self._queue_item = dataclasses.replace(
                    static_definitions.job_definitions[
                        self.job.job_definition_name
                    ].template,
                    name=JobId(self.name),
                ).with_state(state)
            else:
                self._queue_item = dataclasses.replace(
                    static_definitions.jobs[self.job.name],
                    name=JobId(self.name),
                ).with_state(state)
        else:
            raise NotImplementedError("Only support Job queue")
