import typing as t

import json
import logging
import os
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager

from saturn_engine.core import TopicMessage

from .resources import BackpressureApiKey
from .resources import TestApiKey


@contextmanager
def open_db(path: str) -> Iterator[sqlite3.Connection]:
    db = sqlite3.connect(path)
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS pipelines(
            timestamp INT,
            pipeline TEXT,
            params TEXT,
            pid INT
        )"""
    )
    with db:
        yield db
    db.close()


def trace_pipeline(pipeline: str, data: t.Any) -> None:
    with open_db("trace.db") as db:
        db.execute(
            """
            INSERT INTO pipelines(timestamp, pipeline, params, pid)
            VALUES(?, ?, ?, ?)
            """,
            (time.time(), pipeline, json.dumps(data), os.getpid()),
        )


def echo(api_key: TestApiKey, **kwargs: t.Any) -> TopicMessage:
    trace_pipeline("echo", {"api_key": api_key.key} | kwargs)
    logging.info("api_key: %s, data: %s", api_key.key, kwargs)
    time.sleep(5)
    return TopicMessage(args=kwargs)


def slow(api_key: BackpressureApiKey, **kwargs: t.Any) -> TopicMessage:
    trace_pipeline("slow", kwargs)
    time.sleep(10)
    return TopicMessage(args=kwargs)


def fast(**kwargs: t.Any) -> TopicMessage:
    trace_pipeline("fast", kwargs)
    return TopicMessage(args=kwargs)
