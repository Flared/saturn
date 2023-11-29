import typing as t
from typing import AsyncContextManager
from typing import Optional

import abc
import asyncio
import contextlib
from collections.abc import AsyncGenerator
from datetime import timedelta

from saturn_engine.core import TopicMessage
from saturn_engine.utils.log import getLogger
from saturn_engine.utils.options import OptionsSchema

TopicOutput = AsyncContextManager[TopicMessage]


class TopicClosedError(Exception):
    pass


class Topic(OptionsSchema):
    name: str = "unnamed-topic"

    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        raise NotImplementedError()
        yield

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        raise NotImplementedError()

    async def open(self) -> None:
        pass

    async def close(self) -> None:
        pass


class BlockingTopic(Topic, abc.ABC):
    def __init__(
        self,
        max_concurrency: int = 1,
        sleep_time: Optional[timedelta] = None,
        *args: t.Any,
        **kwargs: t.Any,
    ):
        self.logger = getLogger(__name__, self)
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.sleep_time: timedelta = sleep_time or timedelta(seconds=0)
        self.event = asyncio.Event()

    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        while True:
            try:
                messages = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.run_once_blocking,
                )
            except Exception:
                self.logger.exception("Topic failed")
                await asyncio.sleep(self.sleep_time.total_seconds())
                continue

            if messages is None:
                break

            for message in messages:
                yield message

            if len(messages) == 0:
                await self.wait()

    async def wait(self) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                self.event.wait(),
                timeout=self.sleep_time.total_seconds(),
            )
        self.event.clear()

    def wake(self) -> None:
        self.event.set()

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        if not wait and self.semaphore.locked():
            return False

        async with self.semaphore:
            return await asyncio.get_event_loop().run_in_executor(
                None,
                self.publish_blocking,
                message,
                wait,
            )

    def run_once_blocking(self) -> Optional[list[TopicOutput]]:
        raise NotImplementedError()

    def publish_blocking(self, message: TopicMessage, wait: bool) -> bool:
        raise NotImplementedError()
