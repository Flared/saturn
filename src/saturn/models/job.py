from typing import Optional
from typing import TypedDict

from sqlalchemy import Column
from sqlalchemy.orm import Mapped
from sqlalchemy.sql.sqltypes import Integer

from .base import Base
from .utils import UTCDateTime


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = Column(Integer, primary_key=True)
    completed_at = Column(UTCDateTime, nullable=True)

    class AsDict(TypedDict):
        id: int
        completed_at: Optional[str]

    def asdict(self) -> AsDict:
        return Job.AsDict(
            id=self.id,
            completed_at=self.completed_at.isoformat() if self.completed_at else None,
        )
