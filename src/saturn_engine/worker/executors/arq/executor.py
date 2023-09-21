import typing as t

import asyncio
import dataclasses

import aioredis.errors
from arq import create_pool
from arq.connections import ArqRedis
from arq.connections import RedisSettings
from arq.jobs import Job
from arq.jobs import JobStatus

from saturn_engine.core import PipelineResults
from saturn_engine.utils.asyncutils import TasksGroup
from saturn_engine.utils.asyncutils import cached_property
from saturn_engine.utils.config import LazyConfig
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.services import Services

from .. import Executor
from . import EXECUTE_FUNC_NAME
from . import QUEUE_TIMEOUT
from . import RESULT_TIMEOUT
from . import healthcheck_interval
from . import healthcheck_key

ARQ_EXECUTOR_NAMESPACE: t.Final[str] = "arq_executor"


class WorkerDiedError(Exception):
    pass


class ARQExecutor(Executor):
    @dataclasses.dataclass
    class Options:
        redis_url: str
        concurrency: int
        queue_name: str = "arq:queue"
        queue_timeout: int = QUEUE_TIMEOUT
        result_timeout: int = RESULT_TIMEOUT

    def __init__(self, options: Options, services: Services) -> None:
        self.logger = getLogger(__name__, self)
        self.options = options
        self.config = LazyConfig([{ARQ_EXECUTOR_NAMESPACE: self.options}])

        if services.s.hooks.executor_initialized:
            self.logger.warning(
                "ARQExecutor does not support executor_initialized hooks"
            )

    @cached_property
    async def redis_queue(self) -> ArqRedis:
        return await create_pool(RedisSettings.from_dsn(self.options.redis_url))

    async def process_message(self, message: ExecutableMessage) -> PipelineResults:
        config = self.config.load_object(message.config)
        options = config.cast_namespace(ARQ_EXECUTOR_NAMESPACE, ARQExecutor.Options)

        try:
            job = await (await self.redis_queue).enqueue_job(
                EXECUTE_FUNC_NAME,
                message.message.as_remote(),
                _expires=options.queue_timeout,
                _queue_name=options.queue_name,
            )
        except (OSError, aioredis.errors.ConnectionClosedError):
            del self.redis_queue
            raise

        tasks = TasksGroup(name=f"saturn.arq.process_message({message.id})")
        result_task: asyncio.Task[PipelineResults] = tasks.create_task(
            job.result(timeout=self.options.result_timeout)
        )
        healthcheck_task = tasks.create_task(self.monitor_job_healthcheck(job))
        async with tasks:
            while not (done := await tasks.wait(remove=False)):
                pass
            if result_task in done:
                tasks.remove(result_task)
                return result_task.result()
            if healthcheck_task in done:
                tasks.remove(healthcheck_task)
                raise WorkerDiedError() from healthcheck_task.exception()
            raise RuntimeError("Unreachable")

    async def monitor_job_healthcheck(self, job: Job) -> None:
        # Wait for the job to not be enqueued.
        while True:
            await asyncio.sleep(healthcheck_interval)
            if await job.status() is not JobStatus.queued:
                break

        # Monitor the job healthcheck.
        while True:
            await asyncio.sleep(healthcheck_interval)
            if not await job._redis.get(healthcheck_key(job.job_id)):
                break

        # Sleep a little, to avoid any race condition where the healthcheck
        # complete before the result is fetch.
        await asyncio.sleep(10)

    @property
    def concurrency(self) -> int:
        return self.options.concurrency
