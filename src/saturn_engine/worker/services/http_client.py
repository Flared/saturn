import aiohttp

from . import MinimalService


class HttpClient(MinimalService):
    name = "http_client"

    async def open(self) -> None:
        self.session = aiohttp.ClientSession()

    async def close(self) -> None:
        await self.session.close()
