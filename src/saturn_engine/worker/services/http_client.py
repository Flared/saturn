import aiohttp


class HttpClient:
    def __init__(self) -> None:
        self.session = aiohttp.ClientSession()

    async def close(self) -> None:
        await self.session.close()
