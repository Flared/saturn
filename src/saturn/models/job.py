from typing import Optional
from typing import TypedDict

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import backref
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import DateTime
from sqlalchemy.sql.sqltypes import Integer

from .base import Base


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = Column(Integer, primary_key=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    queue_id = Column(Integer, ForeignKey("queues.id"), nullable=False)
    queue: "Queue" = relationship(
        "Queue",
        uselist=False,
        backref=backref("job", uselist=False),
    )

    def __init__(self, *, queue_id: int) -> None:
        self.queue_id = queue_id

    class AsDict(TypedDict):
        id: int
        completed_at: Optional[str]

    def asdict(self) -> AsDict:
        return Job.AsDict(
            id=self.queue.id,
            completed_at=self.completed_at.isoformat() if self.completed_at else None,
        )


from .queue import Queue
