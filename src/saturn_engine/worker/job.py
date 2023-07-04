from collections.abc import AsyncGenerator

import asyncstdlib as alib

from saturn_engine.core import MessageId
from saturn_engine.core import TopicMessage
from saturn_engine.core.api import QueueItemWithState
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.job_state.service import JobStateService

from .inventories import Inventory
from .topics import Topic
from .topics import TopicOutput


class Job(Topic):
    def __init__(
        self,
        *,
        inventory: Inventory,
        queue_item: QueueItemWithState,
        services: Services
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.inventory = inventory
        self.services = services
        self.batch_size = 10
        self.queue_item = queue_item
        self.state_service = services.cast_service(JobStateService)

    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        try:
            cursor = self.queue_item.state.cursor
            done = None

            async with alib.scoped_iter(
                self.inventory.iterate(after=cursor)
            ) as iterator:
                while done is not True:
                    try:
                        done = True
                        async for item in alib.islice(iterator, self.batch_size):
                            cursor = item.cursor
                            done = False
                            message = TopicMessage(
                                id=MessageId(item.id),
                                args=item.args,
                                tags=item.tags,
                                metadata=item.metadata,
                            )
                            yield message
                    finally:
                        if not done and cursor is not None:
                            self.state_service.set_job_cursor(
                                self.queue_item.name, cursor=cursor
                            )

            self.state_service.set_job_completed(self.queue_item.name)
        except Exception as e:
            self.logger.exception("Exception raised from job")
            self.state_service.set_job_failed(self.queue_item.name, error=e)
