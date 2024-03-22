import typing as t

import sqlite3

from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.orm import Session
from sqlalchemy.orm import scoped_session

AnySyncSession = t.Union[Session, scoped_session]
AnySession = AnySyncSession


def upsert(
    session: AnySession,
) -> t.Callable[[t.Any], postgresql.Insert | sqlite.Insert]:
    if not session.bind:
        raise ValueError("Session is unbound")

    if isinstance(session.bind.dialect, postgresql.dialect):
        return postgresql.insert
    elif isinstance(session.bind.dialect, sqlite.dialect):
        return sqlite.insert
    raise ValueError(f"Dialect {session.bind.dialect} not supported")


def is_sqlite3_connection(connection: t.Any) -> bool:
    from sqlalchemy.dialects.sqlite import aiosqlite  # type: ignore

    return isinstance(
        connection,
        (
            aiosqlite.AsyncAdapt_aiosqlite_connection,
            sqlite3.Connection,
        ),
    )
