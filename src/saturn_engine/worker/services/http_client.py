import typing as t
import aiohttp
from saturn_engine.client.worker_manager import WorkerManagerClient

from saturn_engine.utils.options import json_serializer

import dataclasses
from . import MinimalService

class HttpClient(MinimalService):
    name = "http_client"

    async def open(self) -> None:
        self.session = aiohttp.ClientSession(json_serialize=json_serializer)

    async def close(self) -> None:
        await self.session.close()
