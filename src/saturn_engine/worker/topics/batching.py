import asyncio
import dataclasses
from collections.abc import AsyncGenerator
from datetime import datetime
from datetime import timedelta

from saturn_engine.core import TopicMessage
from saturn_engine.core.api import TopicItem
from saturn_engine.utils.asyncutils import TasksGroup
from saturn_engine.utils.options import asdict
from saturn_engine.worker.services import Services
from saturn_engine.worker.topic import TopicOutput

from . import Topic


@dataclasses.dataclass
class BatchMessage:
    batch: list[TopicMessage]


class BatchingTopic(Topic):
    @dataclasses.dataclass
    class Options:
        topic: TopicItem
        batch_size: int = 10
        flush_timeout: timedelta = timedelta(seconds=10)

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        self.options = options
        self.queue: asyncio.Queue[TopicOutput] = asyncio.Queue(
            maxsize=self.options.batch_size
        )

        from saturn_engine.worker.work_factory import build_topic

        self.task_group = TasksGroup(name=f"batching-topic({options.topic.name})")
        self.topic = build_topic(self.options.topic, services=services)
        self.batch: list[dict] = []
        self.start_time: datetime = datetime.utcnow()

    @property
    def _is_done(self) -> bool:
        return self.force_done and self.queue.empty()

    async def _flush(self) -> AsyncGenerator[TopicMessage, None]:
        self.start_time = datetime.utcnow()

        if self.batch:
            yield TopicMessage(args={"batch": self.batch[: self.options.batch_size]})
            self.batch = self.batch[self.options.batch_size :]

    async def _run_topic_task(self) -> AsyncGenerator[TopicMessage, None]:
        self.start_time = datetime.utcnow()
        self.batch = []

        while not self._is_done:
            must_flush = False

            try:
                time_left: float = (
                    self.options.flush_timeout - (datetime.utcnow() - self.start_time)
                ).total_seconds()

                message = await asyncio.wait_for(self.queue.get(), timeout=time_left)
                self.batch.append(asdict(message))

                if len(self.batch) >= self.options.batch_size:
                    must_flush = True
            except asyncio.TimeoutError:
                must_flush = True

            if must_flush:
                async for message in self._flush():
                    yield message

        async for message in self._flush():
            yield message

    async def _read_topic(self) -> None:
        try:
            async for message in self.topic.run():
                await self.queue.put(message)
        finally:
            self.force_done = True

    async def run(self) -> AsyncGenerator[TopicMessage, None]:
        self.force_done = False
        self.task_group.create_task(self._read_topic())

        async for message in self._run_topic_task():
            yield message

    async def close(self) -> None:
        self.force_done = True
        await self.topic.close()
        await self.task_group.close()
