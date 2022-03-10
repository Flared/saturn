from typing import Generic
from typing import Optional

from collections import Counter
from collections.abc import AsyncGenerator

from saturn_engine.core import PipelineResults
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services.hooks import MessagePublished
from saturn_engine.worker.topic import Topic

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
        self.services.hooks.output_blocked.register(self.on_output_blocked)

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
    ) -> AsyncGenerator[None, PipelineResults]:
        params = {"pipeline": message.info.name}
        await self.incr("message.executed.before", params=params)
        try:
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
        params = {"pipeline": event.message.info.name, "topic": event.topic.name}
        await self.incr("message.published.before", params=params)
        try:
            yield
            await self.incr("message.published.success", params=params)
        except Exception:
            await self.incr("message.published.failed", params=params)

    async def on_output_blocked(self, topic: Topic) -> None:
        params = {"topic": topic.name}
        await self.incr("topic.blocked", params=params)

    async def incr(
        self, key: str, *, count: int = 1, params: Optional[dict[str, str]] = None
    ) -> None:
        pass
