from typing import Optional

import dataclasses

from sqlalchemy import Column
from sqlalchemy import Text
from sqlalchemy.orm import Mapped
from sqlalchemy.sql.sqltypes import DateTime

from saturn_engine.core.api import QueueItem
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions

from .base import Base


class Queue(Base):
    __tablename__ = "queues"

    name: Mapped[str] = Column(Text, primary_key=True)
    assigned_at = Column(DateTime(timezone=True))
    assigned_to = Column(Text)
    job: Optional["Job"]
    _queue_item: Optional[QueueItem] = None

    @property
    def queue_item(self) -> QueueItem:
        if self._queue_item is None:
            raise ValueError("Must .join_definitions() first")
        return self._queue_item

    def join_definitions(self, static_definitions: StaticDefinitions) -> None:
        if self.job:
            self._queue_item = dataclasses.replace(
                static_definitions.job_definitions[
                    self.job.job_definition_name
                ].template,
                name=self.name,
            )
        else:
            raise NotImplementedError("Only support Job queue")


from .job import Job
