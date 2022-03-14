from typing import Optional

from itertools import chain
from urllib.parse import quote

import aiodogstatsd

from .. import BaseServices
from .base import BaseMetricsService


def escape_metric(metric: str) -> str:
    return quote(metric).replace(".", "_")


def format_metric_tags(metric: str, tags: dict[str, str]) -> str:
    return ".".join(
        chain(
            [metric],
            (
                f"by_{escape_metric(k)}.{escape_metric(v)}"
                for k, v in sorted(tags.items())
            ),
        )
    )


class StatsdMetrics(BaseMetricsService[BaseServices, "StatsdMetrics.Options"]):
    name = "statsd_metrics"

    class Options:
        host: str = "127.0.0.1"
        port: int = 8125
        namespace: str = "saturn"
        tags_in_metric: bool = False

    async def open(self) -> None:
        await super().open()
        self.client = aiodogstatsd.Client(
            host=self.options.host,
            port=self.options.port,
            namespace=self.options.namespace,
        )
        await self.client.connect()

    async def close(self) -> None:
        await super().close()
        await self.client.close()

    async def incr(
        self, key: str, *, count: int = 1, params: Optional[dict[str, str]] = None
    ) -> None:
        if self.options.tags_in_metric:
            self.client.increment(key, value=count)
            if params:
                key = format_metric_tags(key, params)
                self.client.increment(key, value=count)
        else:
            self.client.increment(key, value=count, tags=params)

    async def timing(
        self, key: str, seconds: float, *, params: Optional[dict[str, str]] = None
    ) -> None:
        value_ms = int(seconds * 1000)
        if self.options.tags_in_metric:
            self.client.timing(key, value=value_ms)
            if params:
                key = format_metric_tags(key, params)
                self.client.timing(key, value=value_ms)
        else:
            self.client.timing(key, value=value_ms, tags=params)
