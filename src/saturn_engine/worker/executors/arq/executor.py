import typing as t

import asyncio
import dataclasses
import pickle  # noqa: S403

from arq import create_pool
from arq.connections import ArqRedis
from arq.connections import RedisSettings
from arq.jobs import Job
from arq.jobs import JobStatus
from opentelemetry.metrics import get_meter
from redis.exceptions import RedisError

from saturn_engine.core import PipelineResults
from saturn_engine.utils.asyncutils import TasksGroup
from saturn_engine.utils.asyncutils import cached_property
from saturn_engine.utils.config import LazyConfig
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.services import Services

from .. import Executor
from . import EXECUTE_FUNC_NAME
from . import TIMEOUT
from . import TIMEOUT_DELAY
from . import executor_healthcheck_key
from . import healthcheck_interval
from . import worker_healthcheck_key

ARQ_EXECUTOR_NAMESPACE: t.Final[str] = "arq_executor"


class WorkerDiedError(Exception):
    pass


class ARQExecutor(Executor):
    @dataclasses.dataclass
    class Options:
        redis_url: str
        concurrency: int
        queue_name: str = "arq:queue"
        timeout: int = TIMEOUT
        timeout_delay: int = TIMEOUT_DELAY

    def __init__(self, options: Options, services: Services) -> None:
        self.logger = getLogger(__name__, self)
        self.options = options
        self.config = LazyConfig([{ARQ_EXECUTOR_NAMESPACE: self.options}])

        meter = get_meter("saturn.metrics")
        self.execute_bytes = meter.create_counter(
            name="saturn.executor.arq.execute",
            unit="By",
            description="""
            Total bytes sent to the executor
            """,
        )

        self.results_bytes = meter.create_counter(
            name="saturn.executor.arq.results",
            unit="By",
            description="""
            Total bytes received from the executor
            """,
        )

        if services.s.hooks.executor_initialized:
            self.logger.warning(
                "ARQExecutor does not support executor_initialized hooks"
            )

    def serialize(self, obj: dict[str, t.Any]) -> bytes:
        data = pickle.dumps(obj)
        self.execute_bytes.add(len(data), {"executor": self.name})
        return data

    def deserialize(self, data: bytes) -> dict[str, t.Any]:
        self.results_bytes.add(len(data), {"executor": self.name})
        obj = pickle.loads(data)  # noqa: S301
        return obj

    @cached_property
    async def redis_queue(self) -> ArqRedis:
        redis_queue = await create_pool(
            RedisSettings.from_dsn(self.options.redis_url),
            job_serializer=self.serialize,
            job_deserializer=self.deserialize,
        )
        await self.init_redis(redis_queue)
        return redis_queue

    async def init_redis(self, redis_queue: ArqRedis) -> None:
        # Clear the executor pending queue, where there might be old work from
        # previous worker instance.
        # WARNING: Assume the worker own the executor.
        await redis_queue.delete(self.options.queue_name)

    async def process_message(self, message: ExecutableMessage) -> PipelineResults:
        config = self.config.load_object(message.config)
        options = config.cast_namespace(ARQ_EXECUTOR_NAMESPACE, ARQExecutor.Options)

        try:
            job = await (await self.redis_queue).enqueue_job(
                EXECUTE_FUNC_NAME,
                message.message.as_remote(),
                _expires=options.timeout + options.timeout_delay,
                _queue_name=options.queue_name,
            )
        except (OSError, RedisError):
            del self.redis_queue
            raise

        tasks = TasksGroup(name=f"saturn.arq.process_message({message.id})")
        result_task: asyncio.Task[PipelineResults] = tasks.create_task(
            job.result(timeout=self.options.timeout)
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
        hc_ex = int((healthcheck_interval * 2) * 1000)
        # Wait for the job to not be enqueued.
        while True:
            await job._redis.psetex(
                worker_healthcheck_key(job.job_id), hc_ex, b"healthy"
            )
            await asyncio.sleep(healthcheck_interval)
            if await job.status() is not JobStatus.queued:
                break

        # Monitor the job healthcheck.
        while True:
            await job._redis.psetex(
                worker_healthcheck_key(job.job_id), hc_ex, b"healthy"
            )
            await asyncio.sleep(healthcheck_interval)
            if not await job._redis.get(executor_healthcheck_key(job.job_id)):
                break

        # Sleep a little, to avoid any race condition where the healthcheck
        # complete before the result is fetch.
        await asyncio.sleep(10)

    @property
    def concurrency(self) -> int:
        return self.options.concurrency
