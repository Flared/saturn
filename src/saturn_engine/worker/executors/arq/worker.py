import typing as t

import asyncio
from concurrent import futures

import arq.worker

from saturn_engine.core import PipelineResults
from saturn_engine.utils.hooks import EventHook
from saturn_engine.worker.pipeline_message import PipelineMessage

from ..bootstrap import PipelineBootstrap
from ..bootstrap import wrap_remote_exception
from . import EXECUTE_TIMEOUT
from . import RESULT_TTL


class Context(t.TypedDict):
    executor: futures.ProcessPoolExecutor
    bootstraper: PipelineBootstrap


async def remote_execute(ctx: Context, message: PipelineMessage) -> PipelineResults:
    executor = ctx["executor"]
    bootstraper = ctx["bootstraper"]
    with wrap_remote_exception():
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, bootstraper.run_pipeline, message)


async def startup(ctx: Context) -> None:
    ctx["executor"] = futures.ProcessPoolExecutor()
    ctx["bootstraper"] = PipelineBootstrap(initialized_hook=EventHook())


async def shutdown(ctx: Context) -> None:
    ctx["executor"].shutdown()


class WorkerSettings:
    functions = [
        arq.worker.func(
            remote_execute,  # type: ignore[arg-type]
            name="remote_execute",
            keep_result=RESULT_TTL,
            timeout=EXECUTE_TIMEOUT,
            max_tries=1,
        )
    ]
    on_startup = startup
    on_shutdown = shutdown
