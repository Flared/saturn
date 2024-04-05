try:
    from sqlalchemy.orm import DeclarativeBase

    class Base(DeclarativeBase):
        pass

except ImportError:
    from sqlalchemy.orm import declarative_base

    Base = declarative_base()  # type: ignore[misc]

__all__ = ("Base",)
