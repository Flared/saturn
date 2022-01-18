from typing import NamedTuple

from saturn_engine.core import PipelineOutput
from saturn_engine.core import PipelineResult
from saturn_engine.utils.hooks import AsyncContextHook
from saturn_engine.utils.hooks import AsyncEventHook
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.topic import Topic


class MessagePublished(NamedTuple):
    message: PipelineMessage
    topic: Topic
    output: PipelineOutput


class Hooks:
    name = "hooks"

    message_polled: AsyncEventHook[PipelineMessage]
    message_scheduled: AsyncEventHook[PipelineMessage]
    message_submitted: AsyncEventHook[PipelineMessage]
    message_executed: AsyncContextHook[PipelineMessage, PipelineResult]
    message_published: AsyncContextHook[MessagePublished, None]

    def __init__(self) -> None:
        self.message_polled = AsyncEventHook()
        self.message_scheduled = AsyncEventHook()
        self.message_submitted = AsyncEventHook()
        self.message_executed = AsyncContextHook()
        self.message_published = AsyncContextHook()
