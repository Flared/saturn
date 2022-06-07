import dataclasses
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
    @dataclasses.dataclass
    class Options:
        redis_url: str
        queue_name: str
        concurrency: int
        queue_timeout: int = QUEUE_TIMEOUT
        result_timeout: int = RESULT_TIMEOUT

    def __init__(self, options: Options, services: Services) -> None:
        self.logger = getLogger(__name__, self)
        self.options = options
        if services.hooks.executor_initialized:
            self.logger.error("ARQExecutor does not support executor_initialized hooks")

    @cached_property
    async def redis_queue(self) -> ArqRedis:
        return await create_pool(RedisSettings.from_dsn(self.options.redis_url))

    async def process_message(self, message: PipelineMessage) -> PipelineResults:
        job = await (await self.redis_queue).enqueue_job(
            self.options.queue_name,
            message,
            _expires=self.options.queue_timeout,
        )
        return await job.result(timeout=self.options.result_timeout)

    @property
    def concurrency(self) -> int:
        return self.options.concurrency
