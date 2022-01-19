from typing import Generic
from typing import Optional

from collections.abc import AsyncGenerator

from saturn_engine.core import PipelineResult
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services.hooks import MessagePublished

from .. import Service
from .. import TOptions
from .. import TServices


class BaseMetricsService(Generic[TServices, TOptions], Service[TServices, TOptions]):
    async def open(self) -> None:
        self.services.hooks.message_polled.register(self.on_message_polled)
        self.services.hooks.message_scheduled.register(self.on_message_scheduled)
        self.services.hooks.message_submitted.register(self.on_message_submitted)
        self.services.hooks.message_executed.register(self.on_message_executed)
        self.services.hooks.message_published.register(self.on_message_published)

    async def on_message_polled(self, message: PipelineMessage) -> None:
        params = {"pipeline": message.info.name}
        await self.incr("message.polled", params=params)

    async def on_message_scheduled(self, message: PipelineMessage) -> None:
        params = {"pipeline": message.info.name}
        await self.incr("message.scheduled", params=params)

    async def on_message_submitted(self, message: PipelineMessage) -> None:
        params = {"pipeline": message.info.name}
        await self.incr("message.submitted", params=params)

    async def on_message_executed(
        self, message: PipelineMessage
    ) -> AsyncGenerator[None, PipelineResult]:
        params = {"pipeline": message.info.name}
        await self.incr("message.executed.before", params=params)
        try:
            yield
            await self.incr("message.executed.success", params=params)
        except Exception:
            await self.incr("message.executed.failed", params=params)

    async def on_message_published(
        self, event: MessagePublished
    ) -> AsyncGenerator[None, None]:
        params = {"pipeline": event.message.info.name, "channel": event.output.channel}
        await self.incr("message.published.before", params=params)
        try:
            yield
            await self.incr("message.published.success", params=params)
        except Exception:
            await self.incr("message.published.failed", params=params)

    async def incr(
        self, key: str, *, count: int = 1, params: Optional[dict[str, str]] = None
    ) -> None:
        pass
