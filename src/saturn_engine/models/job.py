from datetime import datetime

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import backref
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import Integer
from sqlalchemy.sql.sqltypes import Text

from saturn_engine.core.api import JobItem

from .base import Base
from .types import UTCDateTime


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=False)
    cursor = Column(Text, nullable=True)
    completed_at: Mapped[datetime] = Column(UTCDateTime, nullable=True)  # type: ignore
    queue_id = Column(Integer, ForeignKey("queues.id"), nullable=False)
    queue: "Queue" = relationship(
        "Queue",
        uselist=False,
        backref=backref("job", uselist=False),
    )

    def __init__(self, *, queue_id: int) -> None:
        self.id = queue_id
        self.queue_id = queue_id

    def as_core_item(self) -> JobItem:
        return JobItem(
            id=self.id,
            completed_at=self.completed_at,
            cursor=self.cursor,
        )


from .queue import Queue
