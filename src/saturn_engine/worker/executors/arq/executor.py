import typing as t

import dataclasses

from arq import create_pool
from arq.connections import ArqRedis
from arq.connections import RedisSettings

from saturn_engine.core import PipelineResults
from saturn_engine.utils.asyncutils import cached_property
from saturn_engine.utils.config import LazyConfig
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.services import Services

from .. import Executor
from . import EXECUTE_FUNC_NAME
from . import QUEUE_TIMEOUT
from . import RESULT_TIMEOUT

ARQ_EXECUTOR_NAMESPACE: t.Final[str] = "arq_executor"


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

        job = await (await self.redis_queue).enqueue_job(
            EXECUTE_FUNC_NAME,
            message.message,
            _expires=options.queue_timeout,
            _queue_name=options.queue_name,
        )
        return await job.result(timeout=self.options.result_timeout)

    @property
    def concurrency(self) -> int:
        return self.options.concurrency
