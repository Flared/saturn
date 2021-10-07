import asyncio

from saturn.core.message import Message

from . import BaseExecutor


class SimpleExecutor(BaseExecutor):
    def __init__(self) -> None:
        pass

    async def submit(self, message: Message) -> None:
        print(message)
        await asyncio.sleep(5)
