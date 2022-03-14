from typing import Optional

import asyncio
import contextlib
import logging
import time
from collections import defaultdict
from io import StringIO

from .. import BaseServices
from .base import BaseMetricsService


class MemoryMetrics(BaseMetricsService[BaseServices, None]):
    name = "memory_metrics"

    async def open(self) -> None:
        await super().open()
        self.start_time = time.time()
        self.logger = logging.getLogger("saturn.metrics")
        self.counters: dict[str, int] = defaultdict(int)
        self.timings: dict[str, float] = defaultdict(float)
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

    async def timing(
        self, key: str, seconds: float, *, params: Optional[dict[str, str]] = None
    ) -> None:
        self.timings[key] += seconds
        if params:
            self.timings[f"{key}{{{stringify_params(params)}}}"] += seconds

    async def print_metrics(self) -> None:
        while True:
            await asyncio.sleep(10)
            uptime = time.time() - self.start_time
            metrics_summary = StringIO()
            metrics_summary.writelines(
                ["Metrics\n", f"  Uptime: {uptime:0.3f} seconds\n", "  Counters\n"]
            )
            metrics_summary.writelines(
                f"    {k}={v}\n" for k, v in sorted(self.counters.items())
            )

            metrics_summary.write("  Timings\n")
            metrics_summary.writelines(
                f"    {k}={v:0.3f}s\n" for k, v in sorted(self.timings.items())
            )

            self.logger.info(metrics_summary.getvalue())


def stringify_params(params: dict[str, str]) -> str:
    return ",".join(f"{k}={v}" for k, v in sorted(params.items()))
