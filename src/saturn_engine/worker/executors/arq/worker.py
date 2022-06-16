import typing as t

import asyncio
from concurrent import futures

import arq.worker

from saturn_engine.core import PipelineResults
from saturn_engine.utils.hooks import EventHook
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services.loggers import logger

from ..bootstrap import PipelineBootstrap
from ..bootstrap import wrap_remote_exception
from . import EXECUTE_FUNC_NAME
from . import EXECUTE_TIMEOUT
from . import RESULT_TTL


class WorkerOptions(t.TypedDict, total=False):
    worker_concurrency: int
    worker_initializer: t.Optional[t.Callable[[PipelineBootstrap], t.Any]]


class Context(WorkerOptions):
    executor: futures.Executor
    bootstraper: PipelineBootstrap


async def remote_execute(ctx: Context, message: PipelineMessage) -> PipelineResults:
    executor = ctx["executor"]
    bootstraper = ctx["bootstraper"]
    with wrap_remote_exception():
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            executor, bootstraper.bootstrap_pipeline, message
        )


async def startup(ctx: Context) -> None:
    worker_concurrency = ctx.get("worker_concurrency", 4)
    worker_initializer = ctx.get("worker_initializer", None)

    executor = futures.ThreadPoolExecutor(max_workers=worker_concurrency)

    ctx["executor"] = executor
    initialize_hook: EventHook[PipelineBootstrap] = EventHook()
    if worker_initializer:
        initialize_hook.register(worker_initializer)
    initialize_hook.register(logger.on_executor_initialized)
    ctx["bootstraper"] = PipelineBootstrap(initialized_hook=initialize_hook)


async def shutdown(ctx: Context) -> None:
    ctx["executor"].shutdown()


class WorkerSettings:
    functions = [
        arq.worker.func(
            remote_execute,  # type: ignore[arg-type]
            name=EXECUTE_FUNC_NAME,
            keep_result=RESULT_TTL,
            timeout=EXECUTE_TIMEOUT,
            max_tries=1,
        )
    ]
    on_startup = startup
    on_shutdown = shutdown
