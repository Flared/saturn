import typing as t

import dataclasses
from collections.abc import AsyncGenerator

import asyncstdlib as alib

from saturn_engine.core import MessageId
from saturn_engine.core import TopicMessage
from saturn_engine.core.api import QueueItemWithState
from saturn_engine.utils import iterators
from saturn_engine.utils.config import LazyConfig
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.inventories import Inventory
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.job_state.service import JobStateService
from saturn_engine.worker.topics import Topic
from saturn_engine.worker.topics import TopicOutput

JOB_NAMESPACE: t.Final[str] = "job"


class Job(Topic):
    @dataclasses.dataclass
    class Options:
        enable_cursors_states: bool = False
        buffer_flush_after: float = 5
        buffer_size: int = 10

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
        self.queue_item = queue_item
        self.state_service = services.cast_service(JobStateService)
        self.config = LazyConfig([self.services.s.config.r, self.queue_item.config])

    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        try:
            async with alib.scoped_iter(self._make_iterator()) as iterator:
                async for item in iterator:
                    cursor = item.cursor
                    message = TopicMessage(
                        id=MessageId(item.id),
                        args=item.args,
                        tags=item.tags,
                        metadata=item.metadata,
                    )

                    yield message

                    if cursor:
                        self.state_service.set_job_cursor(
                            self.queue_item.name, cursor=cursor
                        )

            self.state_service.set_job_completed(self.queue_item.name)
        except Exception as e:
            self.logger.exception("Exception raised from job")
            self.state_service.set_job_failed(self.queue_item.name, error=e)

    def _make_iterator(self) -> t.AsyncIterator[Item]:
        cursor = self.queue_item.state.cursor
        iterator = self.inventory.iterate(after=cursor)

        # If we enable cursors states, we are going to decorate the iterator
        # to buffer multiple messages together and load their states.
        if self.options.enable_cursors_states:
            buffered_iterator = iterators.async_buffered(
                iterator,
                flush_after=self.options.buffer_flush_after,
                buffer_size=self.options.buffer_size,
            )
            iterator = iterators.async_flatten(buffered_iterator)
        return iterator

    @property
    def options(self) -> Options:
        return self.config.cast_namespace(JOB_NAMESPACE, self.Options)
