import typing as t

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from saturn_engine.core import TopicMessage
from saturn_engine.core.api import QueueItemWithState
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.inventories import Inventory
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.job_state.service import JobStateService
from saturn_engine.worker.topics import Topic
from saturn_engine.worker.topics import TopicOutput

JOB_NAMESPACE: t.Final[str] = "job"


class Job(Topic):
    def __init__(
        self,
        *,
        inventory: Inventory,
        queue_item: QueueItemWithState,
        services: Services,
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.inventory = inventory
        self.services = services
        self.queue_item = queue_item
        self.state_service = services.cast_service(JobStateService)

    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        log_context = {
            "data": {
                "job": {"name": self.queue_item.name},
                "pipeline": self.queue_item.pipeline.info.name,
            }
        }
        cursor = self.queue_item.state.cursor

        try:
            async for item in self.inventory.run(after=cursor):
                yield self.item_to_topic(item)

            self.logger.info("Job inventory completed", extra=log_context)
            await self.inventory.join()
            self.state_service.set_job_completed(self.queue_item.name)
            self.logger.info("Job completed", extra=log_context)
        except Exception as e:
            self.logger.exception("Exception raised from job", extra=log_context)
            self.state_service.set_job_failed(self.queue_item.name, error=e)

    @asynccontextmanager
    async def item_to_topic(self, item_ctx: Item) -> t.AsyncIterator[TopicMessage]:
        try:
            async with item_ctx as item:
                yield item.as_topic_message()
        finally:
            if self.inventory.cursor is not None:
                self.state_service.set_job_cursor(
                    self.queue_item.name, cursor=self.inventory.cursor
                )

    async def open(self) -> None:
        await self.inventory.open()
