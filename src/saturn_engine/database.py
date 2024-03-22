from typing import Any
from typing import Callable
from typing import Iterator
from typing import Optional

from contextlib import contextmanager

import sqlalchemy.orm
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import scoped_session as _sqlalchemy_scoped_session
from sqlalchemy.orm import sessionmaker

from saturn_engine.models import Base
from saturn_engine.utils import lazy
from saturn_engine.utils.inspect import import_name
from saturn_engine.utils.sqlalchemy import AnySyncSession
from saturn_engine.utils.sqlalchemy import is_sqlite3_connection
from saturn_engine.worker_manager.app import current_app


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection: Any, connection_record: Any) -> None:
    if is_sqlite3_connection(dbapi_connection):
        # Enables foreign key support for sqlite.
        cursor = dbapi_connection.cursor()
        cursor.execute("pragma foreign_keys=on;")
        cursor.close()


def init() -> None:
    sqlalchemy.orm.configure_mappers()


def create_all() -> None:
    Base.metadata.create_all(engine())


def drop_all() -> None:
    Base.metadata.drop_all(engine())


def engine() -> Engine:
    init()
    database_connection_creator: Optional[str] = (
        current_app.saturn.config.database_connection_creator
    )
    extra_args = {}
    if database_connection_creator:
        extra_args["creator"] = import_name(database_connection_creator)
    return create_engine(
        current_app.saturn.config.database_url,
        future=True,
        pool_recycle=current_app.saturn.config.database_pool_recycle,
        pool_pre_ping=current_app.saturn.config.database_pool_pre_ping,
        **extra_args,
    )


@lazy()
def session_factory() -> Callable[[], Session]:
    return sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine(),
        future=True,
    )


@contextmanager
def session_scope(
    session_factory: Optional[Callable[[], AnySyncSession]] = None,
) -> Iterator[AnySyncSession]:
    """Provide a transactional scope around a series of operations."""
    session_factory = session_factory or scoped_session
    s = session_factory()
    try:
        yield s
        s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()


@lazy(threadlocal=True)
def scoped_session() -> _sqlalchemy_scoped_session:
    return _sqlalchemy_scoped_session(
        session_factory=session_factory(),
    )


def session() -> Session:
    return session_factory()()
