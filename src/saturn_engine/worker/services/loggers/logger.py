from typing import Any

import logging
from collections.abc import AsyncGenerator

from saturn_engine.core import PipelineOutput
from saturn_engine.core import PipelineResult
from saturn_engine.core import TopicMessage
from saturn_engine.core.api import QueueItem
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services.hooks import MessagePublished
from saturn_engine.worker.work_item import WorkItem

from .. import BaseServices
from .. import Service


class Logger(Service[BaseServices, "Logger.Options"]):
    name = "logger"

    class Options:
        verbose: bool = False

    async def open(self) -> None:
        self.message_logger = logging.getLogger("saturn.messages")
        self.engine_logger = logging.getLogger("saturn.engine")

        self.services.hooks.hook_failed.register(self.on_hook_failed)

        self.services.hooks.work_queue_built.register(self.on_work_queue_built)

        self.services.hooks.message_polled.register(self.on_message_polled)
        self.services.hooks.message_scheduled.register(self.on_message_scheduled)
        self.services.hooks.message_submitted.register(self.on_message_submitted)
        self.services.hooks.message_executed.register(self.on_message_executed)
        self.services.hooks.message_published.register(self.on_message_published)

    async def on_hook_failed(self, error: Exception) -> None:
        self.engine_logger.error("Exception raised in hook", exc_info=error)

    async def on_work_queue_built(
        self, item: QueueItem
    ) -> AsyncGenerator[None, WorkItem]:
        try:
            yield
        except Exception:
            self.engine_logger.exception(
                "Failed to build item", extra={"data": self.queue_item_data(item)}
            )

    async def on_message_polled(self, message: PipelineMessage) -> None:
        self.message_logger.debug(
            "Polled message", extra={"data": self.message_data(message)}
        )

    async def on_message_scheduled(self, message: PipelineMessage) -> None:
        self.message_logger.debug(
            "Scheduled message", extra={"data": self.message_data(message)}
        )

    async def on_message_submitted(self, message: PipelineMessage) -> None:
        self.message_logger.debug(
            "Submitted message", extra={"data": self.message_data(message)}
        )

    async def on_message_executed(
        self, message: PipelineMessage
    ) -> AsyncGenerator[None, PipelineResult]:
        self.message_logger.debug(
            "Executing message", extra={"data": self.message_data(message)}
        )
        try:
            result = yield
            self.message_logger.debug(
                "Executed message",
                extra={
                    "data": self.message_data(message)
                    | {
                        "result": self.result_data(result),
                    }
                },
            )
        except Exception:
            self.message_logger.exception(
                "Failed to execute message", extra={"data": self.message_data(message)}
            )

    async def on_message_published(
        self, event: MessagePublished
    ) -> AsyncGenerator[None, None]:
        self.message_logger.debug(
            "Publishing message", extra={"data": self.published_data(event)}
        )
        try:
            yield
            self.message_logger.debug(
                "Published message", extra={"data": self.published_data(event)}
            )
        except Exception:
            self.message_logger.exception(
                "Failed to publish message", extra={"data": self.published_data(event)}
            )

    def message_data(self, message: PipelineMessage) -> dict[str, Any]:
        return {
            "message": self.topic_message_data(message.message),
            "pipeline": message.info.name,
        }

    def published_data(self, event: MessagePublished) -> dict[str, Any]:
        return {"from": self.message_data(event.message)} | self.output_data(
            event.output
        )

    def topic_message_data(self, message: TopicMessage) -> dict[str, Any]:
        return {
            "id": message.id,
        } | ({"args": message.args} if self.options.verbose else {})

    def result_data(self, results: PipelineResult) -> list[dict[str, Any]]:
        return [self.output_data(o) for o in results.outputs]

    def output_data(self, output: PipelineOutput) -> dict[str, Any]:
        return {
            "channel": output.channel,
            "message": self.topic_message_data(output.message),
        }

    def queue_item_data(self, item: QueueItem) -> dict[str, Any]:
        return {
            "name": item.name,
        }
