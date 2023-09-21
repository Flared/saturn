import typing as t

import asyncio
import concurrent.futures
import dataclasses
import enum
import os
from functools import partial

from saturn_engine.core import PipelineResults
from saturn_engine.utils.hooks import EventHook
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services import Services

from . import Executor
from .bootstrap import PipelineBootstrap
from .bootstrap import wrap_remote_exception

_bootstraper = None


class PoolType(enum.Enum):
    PROCESS = "process"
    THREAD = "thread"


def process_initializer(
    *,
    executor_initialized: EventHook[PipelineBootstrap],
    pool_type: PoolType,
) -> None:
    global _bootstraper

    if pool_type is PoolType.PROCESS:
        # Ignore signals in the process pool since we handle it from the worker
        # process.
        import signal

        signal.signal(signal.SIGINT, signal.SIG_IGN)

    _bootstraper = PipelineBootstrap(initialized_hook=executor_initialized)


class ProcessExecutor(Executor):
    @dataclasses.dataclass
    class Options:
        max_workers: t.Optional[int] = None
        pool_type: PoolType = PoolType.PROCESS

    def __init__(self, options: Options, services: Services) -> None:
        self.max_workers = options.max_workers or os.cpu_count() or 1

        pool_cls: t.Union[
            t.Type[concurrent.futures.ProcessPoolExecutor],
            t.Type[concurrent.futures.ThreadPoolExecutor],
        ]
        if options.pool_type is PoolType.PROCESS:
            pool_cls = concurrent.futures.ProcessPoolExecutor
        elif options.pool_type is PoolType.THREAD:
            pool_cls = concurrent.futures.ThreadPoolExecutor

        self.pool_executor = pool_cls(
            max_workers=self.max_workers,
            initializer=partial(
                process_initializer,
                executor_initialized=services.s.hooks.executor_initialized,
                pool_type=options.pool_type,
            ),
        )

    async def process_message(self, message: ExecutableMessage) -> PipelineResults:
        loop = asyncio.get_running_loop()
        execute = partial(self.remote_execute, message=message.message.as_remote())
        return await loop.run_in_executor(self.pool_executor, execute)

    @property
    def concurrency(self) -> int:
        return self.max_workers

    async def close(self) -> None:
        self.pool_executor.shutdown(wait=False, cancel_futures=True)

    @staticmethod
    def remote_execute(message: PipelineMessage) -> PipelineResults:
        if not _bootstraper:
            raise ValueError("process_initializer must be called")
        with wrap_remote_exception():
            return _bootstraper.bootstrap_pipeline(message)
