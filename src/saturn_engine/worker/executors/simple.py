from saturn_engine.core.message import Message
from saturn_engine.utils.log import getLogger

from . import BaseExecutor


class SimpleExecutor(BaseExecutor):
    def __init__(self) -> None:
        self.logger = getLogger(__name__, self)

    async def process_message(self, message: Message) -> None:
        self.logger.info(message)
