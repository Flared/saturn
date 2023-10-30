import typing as t

import asyncio
import enum
import multiprocessing
from concurrent import futures

import arq.worker
from arq.connections import ArqRedis

from saturn_engine.core import PipelineResults
from saturn_engine.utils.asyncutils import TasksGroup
from saturn_engine.utils.hooks import EventHook
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services import tracing
from saturn_engine.worker.services.loggers import logger

from ..bootstrap import PipelineBootstrap
from ..bootstrap import wrap_remote_exception
from . import EXECUTE_FUNC_NAME
from . import healthcheck_interval
from . import healthcheck_key


class WorkerType(enum.Enum):
    PROCESS = "process"
    THREAD = "thread"


class WorkerOptions(t.TypedDict, total=False):
    worker_concurrency: int
    worker_initializer: t.Optional[t.Callable[[PipelineBootstrap], t.Any]]
    worker_type: WorkerType


class Context(WorkerOptions):
    executor: futures.Executor
    redis: ArqRedis
    job_id: str


_bootstraper: t.Optional[PipelineBootstrap] = None


def _init_bootstraper(
    worker_initializer: t.Optional[t.Callable[[PipelineBootstrap], t.Any]]
) -> None:
    global _bootstraper

    initialize_hook: EventHook[PipelineBootstrap] = EventHook()

    # Arq doesn't support passing hook, so we hard code service hooks here.
    # Would be nice to find a way around that eventually.
    initialize_hook.register(logger.on_executor_initialized)
    initialize_hook.register(tracing.on_executor_initialized)

    if worker_initializer:
        initialize_hook.register(worker_initializer)

    _bootstraper = PipelineBootstrap(initialized_hook=initialize_hook)


def _ensure_bootstraper() -> PipelineBootstrap:
    if _bootstraper is None:
        raise ValueError("Bootstraper wasn't initialized")
    return _bootstraper


def remote_bootstrap(message: PipelineMessage) -> PipelineResults:
    bootstraper = _ensure_bootstraper()
    return bootstraper.bootstrap_pipeline(message)


async def remote_execute(ctx: Context, message: PipelineMessage) -> PipelineResults:
    executor = ctx["executor"]
    with wrap_remote_exception():
        loop = asyncio.get_running_loop()
        tasks = TasksGroup(name=f"saturn.arq.remote({message.id})")

        async def execute_message() -> PipelineResults:
            return await loop.run_in_executor(executor, remote_bootstrap, message)

        executor_task = tasks.create_task(execute_message())
        tasks.create_task(remote_job_healthcheck(ctx))
        async with tasks:
            while not (done := await tasks.wait(remove=False)):
                pass
            if executor_task in done:
                tasks.remove(executor_task)
                return executor_task.result()
            raise RuntimeError("Healthcheck should not finish")


async def remote_job_healthcheck(ctx: Context) -> None:
    while True:
        await ctx["redis"].psetex(
            healthcheck_key(ctx["job_id"]),
            int((healthcheck_interval * 2) * 1000),
            b"healthy",
        )
        await asyncio.sleep(healthcheck_interval)


def remote_initializer(
    worker_initializer: t.Optional[t.Callable[[PipelineBootstrap], t.Any]]
) -> None:
    _init_bootstraper(worker_initializer)


async def startup(ctx: Context) -> None:
    worker_concurrency = ctx.get("worker_concurrency", 4)
    worker_initializer = ctx.get("worker_initializer", None)
    worker_type = ctx.get("worker_type", WorkerType.THREAD)

    if "executor" not in ctx:
        if worker_type is WorkerType.PROCESS:
            context = multiprocessing.get_context("spawn")
            executor = futures.ProcessPoolExecutor(
                max_workers=worker_concurrency,
                initializer=remote_initializer,
                initargs=(worker_initializer,),
                mp_context=context,
            )
        else:
            executor = futures.ThreadPoolExecutor(
                max_workers=worker_concurrency, thread_name_prefix="saturn-arq-"
            )

        ctx["executor"] = executor

    _init_bootstraper(worker_initializer)


async def shutdown(ctx: Context) -> None:
    ctx["executor"].shutdown()


class WorkerSettings:
    functions = [
        arq.worker.func(
            remote_execute,  # type: ignore[arg-type]
            name=EXECUTE_FUNC_NAME,
            max_tries=1,
        )
    ]
    on_startup = startup
    on_shutdown = shutdown
