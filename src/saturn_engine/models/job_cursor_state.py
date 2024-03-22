from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.sqltypes import Text
from sqlalchemy.types import JSON

from .base import Base


class JobCursorState(Base):
    __tablename__ = "job_cursor_states"
    job_definition_name: Mapped[str] = mapped_column(Text, primary_key=True)
    cursor: Mapped[str] = mapped_column(Text, primary_key=True)
    state: Mapped[dict] = mapped_column(JSON, nullable=False)
