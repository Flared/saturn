try:
    from sqlalchemy.orm import mapped_column
except ImportError:
    from sqlalchemy import Column

    mapped_column = Column  # type: ignore[assignment]

__all__ = ("mapped_column",)
