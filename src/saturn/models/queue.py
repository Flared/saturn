from typing import Optional

from sqlalchemy import Column
from sqlalchemy import Text
from sqlalchemy.orm import Mapped
from sqlalchemy.sql.sqltypes import DateTime
from sqlalchemy.sql.sqltypes import Integer

from .base import Base


class Queue(Base):
    __tablename__ = "queues"

    id: Mapped[int] = Column(Integer, primary_key=True)
    assigned_at = Column(DateTime(timezone=True))
    assigned_to = Column(Text)
    pipeline: Mapped[str] = Column(Text, nullable=False)
    job: Optional["Job"]

    def __init__(self, pipeline: str) -> None:
        self.pipeline = pipeline

    @property
    def work_type(self) -> str:
        return "job" if self.job else "queue"

    def as_work_item(self) -> dict:
        return {
            self.work_type: {
                "pipeline": self.pipeline,
                "id": self.id,
            }
        }


from .job import Job
