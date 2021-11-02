from typing import AsyncContextManager

from saturn_engine.core import PipelineMessage
from saturn_engine.worker.parkers import Parkers


class ExecutableMessage:
    def __init__(
        self,
        *,
        message: PipelineMessage,
        parker: Parkers,
        message_context: AsyncContextManager
    ):
        self.message = message
        self.message_context = message_context
        self.parker = parker

    def park(self) -> None:
        self.parker.park(id(self))

    async def unpark(self) -> None:
        await self.parker.unpark(id(self))
