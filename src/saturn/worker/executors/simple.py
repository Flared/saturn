from saturn.core.message import Message
from saturn.utils.log import getLogger

from . import BaseExecutor


class SimpleExecutor(BaseExecutor):
    def __init__(self) -> None:
        self.logger = getLogger(__name__, self)

    async def submit(self, message: Message) -> None:
        self.logger.info(message)
