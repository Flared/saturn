from typing import Generic
from typing import Optional

import abc
import asyncio
from collections import Counter
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from saturn_engine.core import PipelineResults
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.services.hooks import MessagePublished
from saturn_engine.worker.topic import Topic

from .. import Service
from .. import TOptions
from .. import TServices


class BaseMetricsService(
    Generic[TServices, TOptions], Service[TServices, TOptions], abc.ABC
):
    async def open(self) -> None:
        self.services.hooks.message_polled.register(self.on_message_polled)
        self.services.hooks.message_scheduled.register(self.on_message_scheduled)
        self.services.hooks.message_submitted.register(self.on_message_submitted)
        self.services.hooks.message_executed.register(self.on_message_executed)
        self.services.hooks.message_published.register(self.on_message_published)
        self.services.hooks.output_blocked.register(self.on_output_blocked)

    async def on_message_polled(self, xmsg: ExecutableMessage) -> None:
        params = {"pipeline": xmsg.message.info.name}
        await self.incr("message.polled", params=params)

    async def on_message_scheduled(self, xmsg: ExecutableMessage) -> None:
        params = {"pipeline": xmsg.message.info.name}
        await self.incr("message.scheduled", params=params)

    async def on_message_submitted(self, xmsg: ExecutableMessage) -> None:
        params = {"pipeline": xmsg.message.info.name}
        await self.incr("message.submitted", params=params)

    async def on_message_executed(
        self, xmsg: ExecutableMessage
    ) -> AsyncGenerator[None, PipelineResults]:
        message = xmsg.message
        params = {"pipeline": message.info.name}
        await self.incr("message.executed.before", params=params)
        try:
            async with self.timeit("message.executed", params=params):
                results = yield
            await self.incr("message.executed.success", params=params)
            for resource in results.resources:
                await self.incr("resource.used", params={"type": resource.type})

            output_counters: Counter[str] = Counter()
            for output in results.outputs:
                output_counters[output.channel] += 1
            for channel, count in output_counters.items():
                await self.incr(
                    "message.executed.outputs",
                    params=params
                    | {
                        "channel": channel,
                    },
                    count=count,
                )
        except Exception:
            await self.incr("message.executed.failed", params=params)

    async def on_message_published(
        self, event: MessagePublished
    ) -> AsyncGenerator[None, None]:
        params = {"pipeline": event.xmsg.message.info.name, "topic": event.topic.name}
        await self.incr("message.published.before", params=params)
        try:
            yield
            await self.incr("message.published.success", params=params)
        except Exception:
            await self.incr("message.published.failed", params=params)

    async def on_output_blocked(self, topic: Topic) -> None:
        params = {"topic": topic.name}
        await self.incr("topic.blocked", params=params)

    @asynccontextmanager
    async def timeit(
        self, key: str, *, params: Optional[dict[str, str]] = None
    ) -> AsyncIterator[None]:
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        try:
            yield
        finally:
            elapsed = loop.time() - started_at
            await self.timing(key, elapsed, params=params)

    @abc.abstractmethod
    async def incr(
        self, key: str, *, count: int = 1, params: Optional[dict[str, str]] = None
    ) -> None:
        pass

    @abc.abstractmethod
    async def timing(
        self, key: str, seconds: float, *, params: Optional[dict[str, str]] = None
    ) -> None:
        pass
