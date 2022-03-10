from typing import Optional

import asyncio
import contextlib
import logging
import time
from collections import defaultdict

from .. import BaseServices
from .base import BaseMetricsService


class MemoryMetrics(BaseMetricsService[BaseServices, None]):
    name = "memory_metrics"

    async def open(self) -> None:
        await super().open()
        self.start_time = time.time()
        self.logger = logging.getLogger("saturn.metrics")
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
            uptime = time.time() - self.start_time
            self.logger.info(f"Uptime: {uptime:0.3f} seconds")
            for k, v in sorted(self.counters.items()):
                self.logger.info(f"{k}={v}")


def stringify_params(params: dict[str, str]) -> str:
    return ",".join(f"{k}={v}" for k, v in sorted(params.items()))
