import typing as t

import dataclasses
import functools
import itertools
import logging
from functools import partial

from saturn_engine.core import PipelineOutput
from saturn_engine.core import PipelineResults
from saturn_engine.core import TopicMessage
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import QueueItemWithState
from saturn_engine.core.pipeline import PipelineEvent
from saturn_engine.utils.hooks import AsyncContextHook
from saturn_engine.utils.hooks import AsyncContextHookEmiter
from saturn_engine.utils.hooks import AsyncEventHook
from saturn_engine.utils.hooks import EventHook
from saturn_engine.utils.hooks import call_async_handlers
from saturn_engine.utils.inspect import import_name
from saturn_engine.utils.log import getLogger
from saturn_engine.utils.options import OptionsSchema

if t.TYPE_CHECKING:
    from saturn_engine.worker.executors.bootstrap import PipelineBootstrap
    from saturn_engine.worker.executors.executable import ExecutableMessage
    from saturn_engine.worker.executors.executable import ExecutableQueue
    from saturn_engine.worker.topic import Topic

T = t.TypeVar("T")


class ResultsProcessed(t.NamedTuple):
    xmsg: "ExecutableMessage"
    results: "PipelineResults"


class MessagePublished(t.NamedTuple):
    xmsg: "ExecutableMessage"
    topic: "Topic"
    output: "PipelineOutput"


class ItemsBatch(t.NamedTuple):
    items: list[TopicMessage]
    job: QueueItem


class PipelineEventsEmitted(t.NamedTuple):
    xmsg: "ExecutableMessage"
    events: list[PipelineEvent]


listfield: t.Callable[[], t.Any] = lambda: dataclasses.field(default_factory=list)


@dataclasses.dataclass
class HooksLists(t.Generic[T]):
    hook_failed: list[T] = listfield()
    items_batched: list[T] = listfield()
    message_polled: list[T] = listfield()
    message_scheduled: list[T] = listfield()
    message_submitted: list[T] = listfield()
    message_executed: list[T] = listfield()
    results_processed: list[T] = listfield()
    message_published: list[T] = listfield()
    output_blocked: list[T] = listfield()
    pipeline_events_emitted: list[T] = listfield()
    work_queue_built: list[T] = listfield()
    executor_initialized: list[T] = listfield()


@functools.cache
def _load_handler(handler: object) -> t.Optional[t.Callable]:
    try:
        if isinstance(handler, str):
            handler = import_name(handler)
        if callable(handler):
            return handler
        else:
            raise ValueError(f"handler is not callable: {handler}")

    except Exception:
        logging.getLogger(__name__).exception(
            "Failed to load hook handler: %s", handler
        )
    return None


class Hooks(OptionsSchema):
    name = "hooks"

    @dataclasses.dataclass
    class Options(HooksLists[str]):
        def load_hooks(self) -> HooksLists[t.Callable]:
            return HooksLists(
                hook_failed=self._load_hooks(self.hook_failed),
                items_batched=self._load_hooks(self.items_batched),
                message_polled=self._load_hooks(self.message_polled),
                message_scheduled=self._load_hooks(self.message_scheduled),
                message_submitted=self._load_hooks(self.message_submitted),
                message_executed=self._load_hooks(self.message_executed),
                message_published=self._load_hooks(self.message_published),
                results_processed=self._load_hooks(self.results_processed),
                output_blocked=self._load_hooks(self.output_blocked),
                pipeline_events_emitted=self._load_hooks(self.pipeline_events_emitted),
                work_queue_built=self._load_hooks(self.work_queue_built),
                executor_initialized=self._load_hooks(self.executor_initialized),
            )

        @staticmethod
        def _load_hooks(hooks: list[str]) -> list[t.Callable]:
            return [import_name(h) for h in hooks]

    hook_failed: AsyncEventHook[Exception]

    items_batched: AsyncEventHook["ItemsBatch"]
    message_polled: AsyncEventHook["ExecutableMessage"]
    message_scheduled: AsyncEventHook["ExecutableMessage"]
    message_submitted: AsyncEventHook["ExecutableMessage"]
    message_executed: AsyncContextHook["ExecutableMessage", "PipelineResults"]
    message_published: AsyncContextHook["MessagePublished", None]
    results_processed: AsyncContextHook["ResultsProcessed", None]
    output_blocked: AsyncContextHook["MessagePublished", None]
    pipeline_events_emitted: AsyncEventHook[PipelineEventsEmitted]

    work_queue_built: AsyncContextHook["QueueItemWithState", "ExecutableQueue"]
    executor_initialized: EventHook["PipelineBootstrap"]

    def __init__(self, options: t.Optional[Options] = None) -> None:
        options = options or self.Options()
        hooks = options.load_hooks()

        self.logger = getLogger(__name__, self)
        self.hook_failed = AsyncEventHook(hooks.hook_failed)
        self.items_batched = AsyncEventHook(
            hooks.items_batched + [self.on_items_batched],
            error_handler=self.hook_failed.emit,
        )
        self.message_polled = AsyncEventHook(
            hooks.message_polled + [self.on_message_polled],
            error_handler=self.hook_failed.emit,
        )
        self.message_scheduled = AsyncEventHook(
            hooks.message_scheduled + [self.on_message_scheduled],
            error_handler=self.hook_failed.emit,
        )
        self.message_submitted = AsyncEventHook(
            hooks.message_submitted + [self.on_message_submitted],
            error_handler=self.hook_failed.emit,
        )
        self.message_executed = AsyncContextHook(
            hooks.message_executed + [self.on_message_executed],
            error_handler=self.hook_failed.emit,
        )
        self.message_published = AsyncContextHook(
            hooks.message_published + [self.on_message_published],
            error_handler=self.hook_failed.emit,
        )
        self.results_processed = AsyncContextHook(
            hooks.results_processed + [self.on_results_processed],
            error_handler=self.hook_failed.emit,
        )
        self.output_blocked = AsyncContextHook(
            hooks.output_blocked, error_handler=self.hook_failed.emit
        )
        self.pipeline_events_emitted = AsyncEventHook(
            hooks.pipeline_events_emitted + [self.on_pipeline_events_emitted],
            error_handler=self.hook_failed.emit,
        )
        self.work_queue_built = AsyncContextHook(
            hooks.work_queue_built + [self.on_work_queue_built],
            error_handler=self.hook_failed.emit,
        )
        self.executor_initialized = EventHook(
            hooks.executor_initialized,
            error_handler=partial(self.remote_hook_failed, name="executor_initialized"),
        )

    # `pipeline_hook_failed` is a static method with no dependency since it
    # might get called in remote process.
    @staticmethod
    def remote_hook_failed(exception: Exception, *, name: str) -> None:
        logger = logging.getLogger(__name__)
        logger.error("Error while handling %s hook", name, exc_info=exception)

    def _loads_handlers(self, handlers: list[str]) -> t.Iterable[t.Callable]:
        for handler in handlers:
            if h := _load_handler(handler):
                yield h

    def _config_handlers(
        self, obj: t.Union["QueueItem", "TopicMessage"], name: str
    ) -> t.Optional[t.Iterable[t.Callable]]:
        handlers = obj.config.get("hooks", {}).get(name, [])
        return self._loads_handlers(handlers) if handlers else None

    def _msg_handlers(
        self, xmsg: "ExecutableMessage", name: str
    ) -> t.Optional[t.Iterable[t.Callable]]:
        queue_handlers = self._config_handlers(xmsg.queue.definition, name=name)
        msg_handlers = self._config_handlers(xmsg.message.message, name=name)
        if not msg_handlers and not queue_handlers:
            return None

        return itertools.chain(
            queue_handlers or (),
            msg_handlers or (),
        )

    async def _on_msg(
        self, msg: "ExecutableMessage", name: str, arg: t.Optional[t.Any] = None
    ) -> None:
        arg = arg or msg
        if handlers := self._msg_handlers(msg, name):
            await call_async_handlers(
                handlers=handlers, arg=arg, error_handler=self.hook_failed.emit
            )

    def _queue_context(
        self, queue_item: "QueueItem", name: str
    ) -> t.Optional[AsyncContextHookEmiter]:
        if handlers := self._config_handlers(queue_item, name):
            return AsyncContextHookEmiter(
                handlers=handlers,
                scope=None,  # type: ignore
                error_handler=self.hook_failed.emit,
            )
        return None

    def _msg_context(
        self, msg: "ExecutableMessage", name: str
    ) -> t.Optional[AsyncContextHookEmiter]:
        if handlers := self._msg_handlers(msg, name):
            return AsyncContextHookEmiter(
                handlers=handlers,
                scope=None,  # type: ignore
                error_handler=self.hook_failed.emit,
            )
        return None

    async def on_items_batched(self, batch: "ItemsBatch") -> None:
        if handlers := self._config_handlers(batch.job, "items_batched"):
            await call_async_handlers(
                handlers=handlers, arg=batch, error_handler=self.hook_failed.emit
            )

    async def on_message_polled(self, xmsg: "ExecutableMessage") -> None:
        await self._on_msg(xmsg, "message_polled")

    async def on_message_scheduled(self, xmsg: "ExecutableMessage") -> None:
        await self._on_msg(xmsg, "message_scheduled")

    async def on_message_submitted(self, xmsg: "ExecutableMessage") -> None:
        await self._on_msg(xmsg, "message_submitted")

    async def on_message_executed(
        self, xmsg: "ExecutableMessage"
    ) -> t.AsyncGenerator[None, "PipelineResults"]:
        if not (context := self._msg_context(xmsg, "message_executed")):
            return

        g = await context.on_call(xmsg)
        try:
            result = yield
            await context.on_result(g, result)
        except Exception as e:
            await context.on_error(g, e)

    async def on_message_published(
        self, msg: "MessagePublished"
    ) -> t.AsyncGenerator[None, None]:
        if not (context := self._msg_context(msg.xmsg, "message_published")):
            return

        g = await context.on_call(msg)
        try:
            result = yield
            await context.on_result(g, result)
        except Exception as e:
            await context.on_error(g, e)

    async def on_results_processed(
        self,
        msg: ResultsProcessed,
    ) -> t.AsyncGenerator[None, None]:
        if not (context := self._msg_context(msg.xmsg, "results_processed")):
            return

        g = await context.on_call(msg)
        try:
            result = yield
            await context.on_result(g, result)
        except Exception as e:
            await context.on_error(g, e)

    async def on_pipeline_events_emitted(self, events: PipelineEventsEmitted) -> None:
        await self._on_msg(events.xmsg, "pipeline_events_emitted", events)

    async def on_work_queue_built(
        self, queue_item: "QueueItemWithState"
    ) -> t.AsyncGenerator[None, "ExecutableQueue"]:
        if not (context := self._queue_context(queue_item, "work_queue_built")):
            return

        g = await context.on_call(queue_item)
        try:
            result = yield
            await context.on_result(g, result)
        except Exception as e:
            await context.on_error(g, e)
