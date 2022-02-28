import asyncio
import concurrent.futures
from functools import partial

from saturn_engine.core import PipelineResults
from saturn_engine.utils.hooks import EventHook
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services import Services

from . import Executor
from .bootstrap import PipelineBootstrap
from .bootstrap import wrap_remote_exception

_boostraper = None


def process_initializer(
    *,
    executor_initialized: EventHook[PipelineBootstrap],
) -> None:
    global _boostraper
    # Ignore signals in the process pool since we handle it from the worker
    # process.
    import signal

    signal.signal(signal.SIGINT, signal.SIG_IGN)

    _boostraper = PipelineBootstrap(initialized_hook=executor_initialized)


class ProcessExecutor(Executor):
    def __init__(self, services: Services) -> None:
        self.pool_executor = concurrent.futures.ProcessPoolExecutor(
            initializer=partial(
                process_initializer,
                executor_initialized=services.hooks.executor_initialized,
            )
        )

    async def process_message(self, message: PipelineMessage) -> PipelineResults:
        loop = asyncio.get_running_loop()
        execute = partial(self.remote_execute, message=message)
        return await loop.run_in_executor(self.pool_executor, execute)

    async def close(self) -> None:
        self.pool_executor.shutdown(wait=False, cancel_futures=True)

    @staticmethod
    def remote_execute(message: PipelineMessage) -> PipelineResults:
        if not _boostraper:
            raise ValueError("process_initializer must be called")
        with wrap_remote_exception():
            return _boostraper.bootstrap_pipeline(message)
