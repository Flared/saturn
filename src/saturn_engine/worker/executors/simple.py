from saturn_engine.core.message import PipelineMessage
from saturn_engine.utils.log import getLogger

from . import Executor


class SimpleExecutor(Executor):
    def __init__(self) -> None:
        self.logger = getLogger(__name__, self)

    async def process_message(self, message: PipelineMessage) -> None:
        self.logger.info(message)
