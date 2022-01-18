from typing import Generic
from typing import Optional

import asyncio
import contextlib
from collections import defaultdict
from collections.abc import AsyncGenerator

from saturn_engine.core import PipelineResult
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services.hooks import MessagePublished

from . import BaseServices
from . import Service
from . import TOptions
from . import TServices


class MetricsService(Generic[TServices, TOptions], Service[TServices, TOptions]):
    async def open(self) -> None:
        self.services.hooks.message_polled.register(self.on_message_polled)
        self.services.hooks.message_scheduled.register(self.on_message_scheduled)
        self.services.hooks.message_submitted.register(self.on_message_submitted)
        self.services.hooks.message_executed.register(self.on_message_executed)
        self.services.hooks.message_published.register(self.on_message_published)

    async def on_message_polled(self, message: PipelineMessage) -> None:
        params = {"pipeline": message.info.name}
        await self.incr("saturn.message.polled", params=params)

    async def on_message_scheduled(self, message: PipelineMessage) -> None:
        params = {"pipeline": message.info.name}
        await self.incr("saturn.message.scheduled", params=params)

    async def on_message_submitted(self, message: PipelineMessage) -> None:
        params = {"pipeline": message.info.name}
        await self.incr("saturn.message.submitted", params=params)

    async def on_message_executed(
        self, message: PipelineMessage
    ) -> AsyncGenerator[None, PipelineResult]:
        params = {"pipeline": message.info.name}
        await self.incr("saturn.message.executed.before", params=params)
        try:
            yield
            await self.incr("saturn.message.executed.success", params=params)
        except Exception:
            await self.incr("saturn.message.executed.failed", params=params)

    async def on_message_published(
        self, event: MessagePublished
    ) -> AsyncGenerator[None, None]:
        params = {"pipeline": event.message.info.name, "channel": event.output.channel}
        await self.incr("saturn.message.published.before", params=params)
        try:
            yield
            await self.incr("saturn.message.published.success", params=params)
        except Exception:
            await self.incr("saturn.message.published.failed", params=params)

    async def incr(
        self, key: str, *, count: int = 1, params: Optional[dict[str, str]] = None
    ) -> None:
        pass


class MemoryMetrics(MetricsService[BaseServices, None]):
    name = "memory_metrics"

    async def open(self) -> None:
        await super().open()
        self.counters: dict[str, int] = defaultdict(int)
        self.printer_task = asyncio.create_task(self.print_metrics())

    async def close(self) -> None:
        self.printer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.printer_task

    async def incr(
        self, key: str, *, count: int = 1, params: Optional[dict[str, str]] = None
    ) -> None:
        self.counters[key] += count
        if params:
            self.counters[f"{key}{{{stringify_params(params)}}}"] += count

    async def print_metrics(self) -> None:
        while True:
            await asyncio.sleep(10)
            for k, v in sorted(self.counters.items()):
                print(f"{k}: {v}")


def stringify_params(params: dict[str, str]) -> str:
    return ",".join(f"{k}={v}" for k, v in sorted(params.items()))
