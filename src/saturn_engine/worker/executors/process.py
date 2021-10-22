import asyncio
import concurrent.futures
import logging
from functools import partial

from saturn_engine.core.message import Message

from . import BaseExecutor


def pipeline(message: Message) -> None:
    logger = logging.getLogger("pipeline")
    logger.info(message)
    import time

    time.sleep(1)
    print(time.time(), message)


class ProcessExecutor(BaseExecutor):
    def __init__(self, concurrency: int = 8) -> None:
        self.pool_executor = concurrent.futures.ProcessPoolExecutor()

    async def process_message(self, message: Message) -> None:
        loop = asyncio.get_running_loop()
        callback = partial(pipeline, message=message)
        await loop.run_in_executor(self.pool_executor, callback)

    async def close(self) -> None:
        self.pool_executor.shutdown(wait=False, cancel_futures=True)
