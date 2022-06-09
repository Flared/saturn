import typing as t

import asyncio
import enum
import multiprocessing
from concurrent import futures

import arq.worker

from saturn_engine.core import PipelineResults
from saturn_engine.utils import assert_never
from saturn_engine.utils.hooks import EventHook
from saturn_engine.worker.pipeline_message import PipelineMessage

from ..bootstrap import PipelineBootstrap
from ..bootstrap import wrap_remote_exception
from . import EXECUTE_FUNC_NAME
from . import EXECUTE_TIMEOUT
from . import RESULT_TTL


class WorkerType(enum.Enum):
    Thread = "thread"
    Process = "process"


class WorkerOptions(t.TypedDict, total=False):
    worker_type: WorkerType
    worker_concurrency: int
    worker_initializer: t.Optional[t.Callable[[], t.Any]]


class Context(WorkerOptions):
    executor: futures.Executor
    bootstraper: PipelineBootstrap


async def remote_execute(ctx: Context, message: PipelineMessage) -> PipelineResults:
    executor = ctx["executor"]
    bootstraper = ctx["bootstraper"]
    with wrap_remote_exception():
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, bootstraper.run_pipeline, message)


async def startup(ctx: Context) -> None:
    worker_type = ctx.get("worker_type", WorkerType.Thread)
    worker_concurrency = ctx.get("worker_concurrency", 4)
    worker_initializer = ctx.get("worker_initializer", None)

    executor: futures.Executor
    if worker_type is WorkerType.Thread:
        executor = futures.ThreadPoolExecutor(
            max_workers=worker_concurrency, initializer=worker_initializer
        )
    elif worker_type is WorkerType.Process:
        context = multiprocessing.get_context("spawn")
        executor = futures.ProcessPoolExecutor(
            max_workers=worker_concurrency,
            mp_context=context,
            initializer=worker_initializer,
        )
    else:
        assert_never(worker_type)

    ctx["executor"] = executor
    ctx["bootstraper"] = PipelineBootstrap(initialized_hook=EventHook())


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
