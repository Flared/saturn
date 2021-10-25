from saturn_engine.core.message import Message
from saturn_engine.utils.log import getLogger

from . import Executor


class SimpleExecutor(Executor):
    def __init__(self) -> None:
        self.logger = getLogger(__name__, self)

    async def submit(self, message: Message) -> None:
        async with message.process():
            self.logger.info(message)
