

import aiohttp

from saturn_engine.utils.options import json_serializer

from . import MinimalService


class HttpClient(MinimalService):
    name = "http_client"

    async def open(self) -> None:
        self.session = aiohttp.ClientSession(json_serialize=json_serializer)

    async def close(self) -> None:
        await self.session.close()
