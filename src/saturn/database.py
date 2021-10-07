from contextlib import contextmanager
from typing import Callable
from typing import Iterator

from sqlalchemy.engine import Engine
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from saturn.models import Base
from saturn.utils import lazy


def create_all() -> None:
    Base.metadata.create_all(bind=engine())


def drop_all() -> None:
    Base.metadata.drop_all(bind=engine())


@lazy
def engine() -> Engine:
    return create_engine("sqlite:///test.db", future=True)


@lazy
def session_factory() -> Callable[[], Session]:
    return sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine(),
        future=True,
    )


@contextmanager
def session_scope() -> Iterator[Session]:
    """Provide a transactional scope around a series of operations."""
    s = session()
    try:
        yield s
        s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()


def session() -> Session:
    return session_factory()()
