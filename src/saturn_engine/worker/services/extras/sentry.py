from typing import Any
from typing import Optional

import logging
from collections.abc import AsyncGenerator

import sentry_sdk
from sentry_sdk import Hub
from sentry_sdk.utils import capture_internal_exceptions
from sentry_sdk.utils import event_from_exception

from saturn_engine.core import PipelineResult
from saturn_engine.core.api import QueueItem
from saturn_engine.utils.options import asdict
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services.hooks import MessagePublished
from saturn_engine.worker.work import WorkItems

from .. import BaseServices
from .. import Service

Event = dict[str, Any]
Hint = dict[str, Any]


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
        self.services.hooks.work_items_built.register(self.on_work_items_built)
        self.services.hooks.message_executed.register(self.on_message_executed)
        self.services.hooks.message_published.register(self.on_message_published)

    def on_before_send(self, event: Event, hint: Hint) -> Event:
        self.logger.debug("Sending event", extra={"data": {"sentry_event": event}})
        pipeline_message = event.get("extra", {}).get("saturn-pipeline-message")
        if pipeline_message:
            self._sanitize_message_resources(pipeline_message)
        return event

    async def on_hook_failed(self, error: Exception) -> None:
        self._capture_exception(error)

    async def on_work_items_built(
        self, item: QueueItem
    ) -> AsyncGenerator[None, WorkItems]:
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
        self, message: PipelineMessage
    ) -> AsyncGenerator[None, PipelineResult]:
        with Hub.current.push_scope() as scope:

            def _event_processor(event: Event, hint: Hint) -> Event:
                with capture_internal_exceptions():
                    tags = event.setdefault("tags", {})
                    tags["saturn_message"] = message.message.id
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
        self, message: MessagePublished
    ) -> AsyncGenerator[None, None]:
        with Hub.current.push_scope() as scope:

            def _event_processor(event: Event, hint: Hint) -> Event:
                with capture_internal_exceptions():
                    tags = event.setdefault("tags", {})
                    tags["saturn_message"] = message.output.message.id
                    tags["saturn_channel"] = message.output.channel
                    tags["saturn_pipeline"] = message.message.info.name
                    extra = event.setdefault("extra", {})
                    extra["saturn-message"] = asdict(message.output.message)
                    extra["saturn-pipeline-message"] = asdict(message.message)
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

        event, hint = event_from_exception(
            exc_info,
            client_options=client.options,
            mechanism={"type": "saturn", "handled": False},
        )

        self.logger.debug("Capturing event", extra={"data": {"sentry_event": event}})
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
