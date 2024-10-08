import typing as t

import asyncio
import dataclasses
import pickle  # noqa: S403

import redis.exceptions
from arq.connections import ArqRedis
from arq.connections import RedisSettings
from arq.jobs import Job
from arq.jobs import JobStatus
from opentelemetry.metrics import get_meter
from redis.asyncio import BlockingConnectionPool
from redis.asyncio import ConnectionPool
from redis.asyncio import SSLConnection
from redis.asyncio import UnixDomainSocketConnection

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
        redis_pool_args: dict[str, t.Any] = dataclasses.field(default_factory=dict)
        redis_conn_retries: int = 5
        redis_conn_retry_delay: float = 1
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

    def create_connection_pool(self) -> ConnectionPool:
        # Create a connection from
        redis_settings = RedisSettings.from_dsn(self.options.redis_url)
        kwargs: dict[str, t.Any] = {
            "username": redis_settings.username,
            "password": redis_settings.password,
            "db": redis_settings.database,
            "socket_connect_timeout": 1,
            "max_connections": self.concurrency,
            "timeout": 10,
        }

        if redis_settings.unix_socket_path is not None:
            kwargs.update(
                {
                    "path": redis_settings.unix_socket_path,
                    "connection_class": UnixDomainSocketConnection,
                }
            )
        else:
            # TCP specific options
            kwargs.update(
                {
                    "host": redis_settings.host,
                    "port": redis_settings.port,
                }
            )
            if redis_settings.ssl:
                kwargs["connection_class"] = SSLConnection

        kwargs.update(self.options.redis_pool_args)
        return BlockingConnectionPool(**kwargs)

    @cached_property
    async def redis_queue(self) -> ArqRedis:
        def make_arq_redis() -> ArqRedis:
            connection_pool = self.create_connection_pool()
            return ArqRedis(
                job_serializer=self.serialize,
                job_deserializer=self.deserialize,
                connection_pool=connection_pool,
            )

        retry = 0
        while True:
            try:
                arq_redis = make_arq_redis()
                await arq_redis.ping()
            except (
                OSError,
                redis.exceptions.RedisError,
                asyncio.TimeoutError,
            ) as e:
                if retry < self.options.redis_conn_retries:
                    self.logger.warning(
                        "redis connection error %s %s, %s retries remaining...",
                        e.__class__.__name__,
                        e,
                        self.options.redis_conn_retries - retry,
                    )
                    await asyncio.sleep(self.options.redis_conn_retry_delay)
                    retry = retry + 1
                else:
                    raise
            else:
                if retry > 0:
                    self.logger.info("redis connection successful")
                await self.init_redis(arq_redis)
                return arq_redis

    async def init_redis(self, redis_queue: ArqRedis) -> None:
        # Clear the executor pending queue, where there might be old work from
        # previous worker instance.
        # WARNING: Assume the worker own the executor.
        await redis_queue.delete(self.options.queue_name)

    async def process_message(self, message: ExecutableMessage) -> PipelineResults:
        config = self.config.load_object(message.config)
        options = config.cast_namespace(ARQ_EXECUTOR_NAMESPACE, ARQExecutor.Options)

        job = await (await self.redis_queue).enqueue_job(
            EXECUTE_FUNC_NAME,
            message.message.as_remote(),
            _expires=options.timeout + options.timeout_delay,
            _queue_name=options.queue_name,
        )

        tasks = TasksGroup(name=f"saturn.arq.process_message({message.id})")
        result_task: asyncio.Task[PipelineResults] = tasks.create_task(
            job.result(timeout=options.timeout)
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

    async def _close_redis(self) -> None:
        await (await self.redis_queue).close(close_connection_pool=True)
        del self.redis_queue

    async def close(self) -> None:
        await self._close_redis()
