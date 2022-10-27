from typing import Any
from typing import Optional
from typing import Type

import logging
import os
from collections.abc import AsyncGenerator
from collections.abc import Iterator
from types import TracebackType

import sentry_sdk
from sentry_sdk import Hub
from sentry_sdk.utils import capture_internal_exceptions
from sentry_sdk.utils import event_from_exception

from saturn_engine.core import PipelineResults
from saturn_engine.core.api import QueueItem
from saturn_engine.utils.options import asdict
from saturn_engine.utils.traceback_data import FrameData
from saturn_engine.utils.traceback_data import TracebackData
from saturn_engine.worker.executors.bootstrap import RemoteException
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.executors.executable import ExecutableQueue
from saturn_engine.worker.services.hooks import MessagePublished

from .. import BaseServices
from .. import Service

Event = dict[str, Any]
Hint = dict[str, Any]
ExcInfo = tuple[
    Optional[Type[BaseException]], Optional[BaseException], Optional[TracebackType]
]


class Sentry(Service[BaseServices, "Sentry.Options"]):
    name = "sentry"

    class Options:
        dsn: Optional[str] = None

    async def open(self) -> None:
        self.logger = logging.getLogger("saturn.extras.sentry")
        sentry_sdk.init(
            self.options.dsn,
            environment=self.services.config.c.env.value,
            before_send=self.on_before_send,
        )

        self.services.hooks.hook_failed.register(self.on_hook_failed)
        self.services.hooks.work_queue_built.register(self.on_work_queue_built)
        self.services.hooks.message_executed.register(self.on_message_executed)
        self.services.hooks.message_published.register(self.on_message_published)

    def on_before_send(self, event: Event, hint: Hint) -> Optional[Event]:
        exc_info = hint.get("exc_info")
        # RemoteException should have been unwrapped in one of the hooks,
        # but some exception might have been catched by other integration such
        # as logging, so we just ignore those.
        if exc_info and isinstance(exc_info[1], RemoteException):
            return None
        pipeline_message = event.get("extra", {}).get("saturn-pipeline-message")
        if pipeline_message:
            self._sanitize_message_resources(pipeline_message)

        self.logger.debug(
            "Sending event", extra={"data": {"event_id": event.get("event_id")}}
        )
        return event

    async def on_hook_failed(self, error: Exception) -> None:
        self._capture_exception(error)

    async def on_work_queue_built(
        self, item: QueueItem
    ) -> AsyncGenerator[None, ExecutableQueue]:
        with Hub.current.push_scope() as scope:

            def _event_processor(event: Event, hint: Hint) -> Event:
                with capture_internal_exceptions():
                    tags = event.setdefault("tags", {})
                    tags["saturn_queue"] = item.name
                    extra = event.setdefault("extra", {})
                    extra["saturn-queue"] = asdict(item)
                return event

            scope.add_event_processor(_event_processor)
            try:
                yield
            except Exception as e:
                self._capture_exception(e)

    async def on_message_executed(
        self, xmsg: ExecutableMessage
    ) -> AsyncGenerator[None, PipelineResults]:
        message = xmsg.message
        with Hub.current.push_scope() as scope:

            def _event_processor(event: Event, hint: Hint) -> Event:
                with capture_internal_exceptions():
                    tags = event.setdefault("tags", {})
                    tags["saturn_message"] = xmsg.id
                    tags["saturn_pipeline"] = message.info.name
                    extra = event.setdefault("extra", {})
                    extra["saturn-pipeline-message"] = asdict(message)
                return event

            scope.add_event_processor(_event_processor)
            try:
                yield
            except Exception as e:
                self._capture_exception(e)

    async def on_message_published(
        self, publish: MessagePublished
    ) -> AsyncGenerator[None, None]:
        with Hub.current.push_scope() as scope:

            def _event_processor(event: Event, hint: Hint) -> Event:
                with capture_internal_exceptions():
                    tags = event.setdefault("tags", {})
                    tags["saturn_message"] = publish.output.message.id
                    tags["saturn_channel"] = publish.output.channel
                    tags["saturn_pipeline"] = publish.xmsg.message.info.name
                    extra = event.setdefault("extra", {})
                    extra["saturn-message"] = asdict(publish.output.message)
                    extra["saturn-pipeline-message"] = asdict(publish.xmsg.message)
                return event

            scope.add_event_processor(_event_processor)
            try:
                yield
            except Exception as e:
                self._capture_exception(e)

    def _capture_exception(self, exc_info: Exception) -> None:
        hub = Hub.current
        client = hub.client
        if not client:
            return

        if isinstance(exc_info, RemoteException):
            event, hint = event_from_remote_exception(
                exc_info,
                client_options=client.options,
                mechanism={"type": "saturn", "handled": False},
            )
        else:
            event, hint = event_from_exception(
                exc_info,
                client_options=client.options,
                mechanism={"type": "saturn", "handled": False},
            )

        hub.capture_event(event, hint=hint)

    @staticmethod
    def _sanitize_message_resources(pipeline_message: dict[str, Any]) -> dict[str, Any]:
        for resource_arg in pipeline_message.get("info", {}).get("resources", {}):
            resource = (
                pipeline_message.get("message", {}).get("args", {}).get(resource_arg)
            )
            resource_name = resource.get("name")
            resource.clear()
            resource["name"] = resource_name
        return pipeline_message


# Following functions are adapted from Sentry-sdk to support
# TracebackData instead of regular Traceback.
def walk_traceback_chain(cause: Optional[TracebackData]) -> Iterator[TracebackData]:
    while cause:
        yield cause

        if cause.__suppress_context__:
            cause = cause.__cause__
        else:
            cause = cause.__context__


def serialize_frame(
    frame: FrameData,
    with_locals: bool = True,
) -> dict[str, Any]:
    abs_path = frame.filename

    rv: dict[str, Any] = {
        "filename": abs_path,
        "abs_path": os.path.abspath(abs_path) if abs_path else None,
        "function": frame.name or "<unknown>",
        "module": frame.module,
        "lineno": frame.lineno,
        "pre_context": frame.lines_before,
        "context_line": frame.line,
        "post_context": frame.lines_after,
    }
    if with_locals:
        rv["vars"] = frame.locals

    return rv


def single_exception_from_remote_error_tuple(
    tb: TracebackData,
    client_options: Optional[dict[str, Any]] = None,
    mechanism: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if client_options is None:
        with_locals = True
    else:
        with_locals = client_options["with_locals"]

    frames = [serialize_frame(frame, with_locals=with_locals) for frame in tb.stack]

    rv = {
        "module": tb.exc_module,
        "type": tb.exc_type,
        "value": tb.exc_str,
        "mechanism": mechanism,
    }

    if frames:
        rv["stacktrace"] = {"frames": frames}

    return rv


def exceptions_from_traceback(
    tb_exc: TracebackData,
    client_options: Optional[dict[str, Any]] = None,
    mechanism: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    rv = []
    for tb in walk_traceback_chain(tb_exc):
        rv.append(
            single_exception_from_remote_error_tuple(tb, client_options, mechanism)
        )

    rv.reverse()

    return rv


def event_from_remote_exception(
    exc: RemoteException,
    client_options: Optional[dict[str, Any]] = None,
    mechanism: Optional[dict[str, Any]] = None,
) -> tuple[Event, Hint]:
    return (
        {
            "level": "error",
            "exception": {
                "values": exceptions_from_traceback(
                    exc.remote_traceback, client_options, mechanism
                )
            },
        },
        {"exc_info": None},
    )
