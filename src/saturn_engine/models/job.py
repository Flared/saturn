from datetime import datetime

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import backref
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import Text

from saturn_engine.core.api import JobItem

from .base import Base
from .types import UTCDateTime


class Job(Base):
    __tablename__ = "jobs"

    name: Mapped[str] = Column(Text, primary_key=True)
    cursor = Column(Text, nullable=True)
    completed_at: Mapped[datetime] = Column(UTCDateTime, nullable=True)  # type: ignore[assignment]  # noqa: B950
    queue_name = Column(Text, ForeignKey("queues.name"), nullable=False)
    queue: "Queue" = relationship(
        "Queue",
        uselist=False,
        backref=backref("job", uselist=False),
    )
    job_definition_name: Mapped[str] = Column(Text, nullable=False)

    def __init__(self, *, queue_name: str, job_definition_name: str) -> None:
        self.name = queue_name
        self.queue_name = queue_name
        self.job_definition_name = job_definition_name

    def as_core_item(self) -> JobItem:
        return JobItem(
            name=self.name,
            completed_at=self.completed_at,
            cursor=self.cursor,
        )


from .queue import Queue
