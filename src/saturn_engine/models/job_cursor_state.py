from sqlalchemy.orm import Mapped
from sqlalchemy.sql.sqltypes import Text
from sqlalchemy.types import JSON

from saturn_engine.models.compat import mapped_column

from .base import Base


class JobCursorState(Base):
    __tablename__ = "job_cursor_states"
    job_definition_name: Mapped[str] = mapped_column(Text, primary_key=True)
    cursor: Mapped[str] = mapped_column(Text, primary_key=True)
    state: Mapped[dict] = mapped_column(JSON, nullable=False)
