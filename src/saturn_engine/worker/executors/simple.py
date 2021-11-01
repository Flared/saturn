from saturn_engine.core.message import PipelineMessage
from saturn_engine.utils.log import getLogger

from . import BaseExecutor


class SimpleExecutor(BaseExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.logger = getLogger(__name__, self)

    async def process_message(self, message: PipelineMessage) -> None:
        self.logger.info(message)
