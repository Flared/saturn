from asyncio import current_task
from contextlib import asynccontextmanager
from typing import Any
from typing import AsyncIterator
from typing import Callable
from typing import Optional
from typing import Union

import sqlalchemy.orm
from sqlalchemy.engine import Engine
from sqlalchemy.engine import create_engine
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import (
    async_scoped_session as _sqlalchemy_async_scoped_session,
)
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from saturn_engine.models import Base
from saturn_engine.utils import lazy
from saturn_engine.worker_manager.config import config

AnyAsyncSession = Union[AsyncSession, _sqlalchemy_async_scoped_session]
AnySession = Union[Session, AnyAsyncSession]

import sqlite3

from sqlalchemy import event


def is_sqlite3_connection(connection: Any) -> bool:
    from sqlalchemy.dialects.sqlite import aiosqlite  # type: ignore

    return isinstance(
        connection,
        (
            aiosqlite.AsyncAdapt_aiosqlite_connection,
            sqlite3.Connection,
        ),
    )


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection: Any, connection_record: Any) -> None:
    if is_sqlite3_connection(dbapi_connection):
        # Enables foreign key support for sqlite.
        cursor = dbapi_connection.cursor()
        cursor.execute("pragma foreign_keys=on;")
        cursor.close()


def init() -> None:
    sqlalchemy.orm.configure_mappers()


async def create_all() -> None:
    async with async_engine().begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def drop_all() -> None:
    async with async_engine().begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@lazy(threadlocal=True)
def async_engine() -> AsyncEngine:
    init()
    async_database_url: str = config().async_database_url
    connect_args: dict = {}
    if "postgresql" in async_database_url:
        # https://magicstack.github.io/asyncpg/current/faq.html
        # See "why-am-i-getting-prepared-statement-errors"
        connect_args["statement_cache_size"] = 0
    return create_async_engine(
        config().async_database_url,
        future=True,
        connect_args=connect_args,
    )


def engine() -> Engine:
    init()
    return create_engine(config().database_url, future=True)


@lazy(threadlocal=True)
def async_session_factory() -> Callable[[], AsyncSession]:
    return sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=async_engine(),
        future=True,
        class_=AsyncSession,
    )


@lazy()
def session_factory() -> Callable[[], Session]:
    return sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine(),
        future=True,
    )


@asynccontextmanager
async def async_session_scope(
    session_factory: Optional[Callable[[], AnyAsyncSession]] = None,
) -> AsyncIterator[AnyAsyncSession]:
    """Provide a transactional scope around a series of operations."""
    session_factory = session_factory or async_scoped_session
    s = session_factory()
    try:
        yield s
        await s.commit()
    except Exception:
        await s.rollback()
        raise
    finally:
        await s.close()


@lazy(threadlocal=True)
def async_scoped_session() -> _sqlalchemy_async_scoped_session:
    return _sqlalchemy_async_scoped_session(
        async_session_factory(),
        scopefunc=current_task,
    )


def session() -> Session:
    return session_factory()()
