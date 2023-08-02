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
        self._pendings: dict[int, tuple[bool, Item]] = {}

    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        cursor = self.queue_item.state.cursor

        try:
            async for item in self.inventory.iterate(after=cursor):
                cursor = item.cursor
                yield self.item_to_topic(item)

            self.state_service.set_job_completed(self.queue_item.name)
        except Exception as e:
            self.logger.exception("Exception raised from job")
            self.state_service.set_job_failed(self.queue_item.name, error=e)

    @asynccontextmanager
    async def item_to_topic(self, item_ctx: Item) -> t.AsyncIterator[TopicMessage]:
        self._set_item_pending(item_ctx)
        try:
            async with item_ctx as item:
                yield item.as_topic_message()
        except Exception:
            self.logger.exception(
                "Failed to process item",
                extra={"data": {"message": {"id": item_ctx.id}}},
            )
        finally:
            self._set_item_done(item_ctx)

    def _set_item_pending(self, item: Item) -> None:
        self._pendings[id(item)] = (True, item)

    def _set_item_done(self, item: Item) -> None:
        self._pendings[id(item)] = (False, item)

        # Collect the serie of done item from the beginning.
        items_done = []
        for pending, item in self._pendings.values():
            if pending:
                break
            items_done.append(item)

        # Remove all done item from the pendings
        for item in items_done:
            del self._pendings[id(item)]

        # Commit last cursor.
        for item in reversed(items_done):
            if item.cursor:
                self.state_service.set_job_cursor(
                    self.queue_item.name, cursor=item.cursor
                )
                break
