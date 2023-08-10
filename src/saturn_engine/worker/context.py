import typing as t

import contextlib
from contextvars import ContextVar

from saturn_engine.core.api import QueueItem
from saturn_engine.core.pipeline import PipelineInfo
from saturn_engine.core.topic import TopicMessage


class ContextVars:
    job: t.Final[ContextVar[t.Optional[QueueItem]]] = ContextVar(
        "saturn.job", default=None
    )
    pipeline: t.Final[ContextVar[t.Optional[PipelineInfo]]] = ContextVar(
        "saturn.pipeline", default=None
    )
    message: t.Final[ContextVar[t.Optional[TopicMessage]]] = ContextVar(
        "saturn.message", default=None
    )

    @classmethod
    def context_summary(cls) -> dict[str, t.Any]:
        summary: dict[str, t.Any] = {}
        if job := cls.job.get():
            summary["job"] = {"name": job.name}

        if pipeline := cls.pipeline.get():
            summary["pipeline"] = pipeline.name

        if message := cls.message.get():
            summary["message"] = {
                "id": message.id,
                "tags": message.tags,
            }

        return summary


@contextlib.contextmanager
def pipeline_context(pipeline: PipelineInfo) -> t.Iterator[None]:
    token = ContextVars.pipeline.set(pipeline)
    try:
        yield
    finally:
        ContextVars.pipeline.reset(token)


@contextlib.contextmanager
def message_context(msg: TopicMessage) -> t.Iterator[None]:
    token = ContextVars.message.set(msg)
    try:
        yield
    finally:
        ContextVars.message.reset(token)


@contextlib.contextmanager
def job_context(job: QueueItem) -> t.Iterator[None]:
    token = ContextVars.job.set(job)
    try:
        with pipeline_context(job.pipeline.info):
            yield
    finally:
        ContextVars.job.reset(token)
