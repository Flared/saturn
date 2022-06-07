from arq import create_pool
from arq.connections import ArqRedis
from arq.connections import RedisSettings

from saturn_engine.core import PipelineResults
from saturn_engine.utils.asyncutils import cached_property
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services import Services

from .. import Executor
from . import QUEUE_TIMEOUT
from . import RESULT_TIMEOUT


class ARQExecutor(Executor):
    def __init__(self, services: Services) -> None:
        self.logger = getLogger(__name__, self)
        self.config = services.s.config.c
        if services.hooks.executor_initialized:
            self.logger.error("ARQExecutor does not support executor_initialized hooks")

    @cached_property
    async def redis_queue(self) -> ArqRedis:
        return await create_pool(RedisSettings.from_dsn(self.config.redis.dsn))

    async def process_message(self, message: PipelineMessage) -> PipelineResults:
        job = await (await self.redis_queue).enqueue_job(
            "remote_execute",
            message,
            _expires=QUEUE_TIMEOUT,
        )
        return await job.result(timeout=RESULT_TIMEOUT)

    @property
    def concurrency(self) -> int:
        return 1
