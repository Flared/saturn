import typing as t

from functools import partial

from saturn_engine.utils.hooks import AsyncContextHook
from saturn_engine.utils.hooks import AsyncEventHook
from saturn_engine.utils.hooks import EventHook

if t.TYPE_CHECKING:
    from saturn_engine.core import PipelineOutput
    from saturn_engine.core import PipelineResults
    from saturn_engine.core.api import QueueItem
    from saturn_engine.worker.executors.bootstrap import PipelineBootstrap
    from saturn_engine.worker.pipeline_message import PipelineMessage
    from saturn_engine.worker.topic import Topic
    from saturn_engine.worker.work_item import WorkItem


class MessagePublished(t.NamedTuple):
    message: "PipelineMessage"
    topic: "Topic"
    output: "PipelineOutput"


class Hooks:
    name = "hooks"

    hook_failed: AsyncEventHook[Exception]

    message_polled: AsyncEventHook["PipelineMessage"]
    message_scheduled: AsyncEventHook["PipelineMessage"]
    message_submitted: AsyncEventHook["PipelineMessage"]
    message_executed: AsyncContextHook["PipelineMessage", "PipelineResults"]
    message_published: AsyncContextHook["MessagePublished", None]

    work_queue_built: AsyncContextHook["QueueItem", "WorkItem"]
    executor_initialized: EventHook["PipelineBootstrap"]

    def __init__(self) -> None:
        self.hook_failed = AsyncEventHook()
        self.work_queue_built = AsyncContextHook(error_handler=self.hook_failed.emit)
        self.message_polled = AsyncEventHook(error_handler=self.hook_failed.emit)
        self.message_scheduled = AsyncEventHook(error_handler=self.hook_failed.emit)
        self.message_submitted = AsyncEventHook(error_handler=self.hook_failed.emit)
        self.message_executed = AsyncContextHook(error_handler=self.hook_failed.emit)
        self.message_published = AsyncContextHook(error_handler=self.hook_failed.emit)
        self.executor_initialized = EventHook(
            error_handler=partial(self.remote_hook_failed, name="executor_initialized")
        )

    # `pipeline_hook_failed` is a static method with no dependency since it
    # might get called in remote process.
    @staticmethod
    def remote_hook_failed(exception: Exception, *, name: str) -> None:
        import logging

        logger = logging.getLogger("saturn.hooks")
        logger.error("Error while handling %s hook", name, exc_info=exception)
