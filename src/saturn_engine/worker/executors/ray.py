from typing import Any

import ray

from saturn_engine.core import PipelineResult
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services.config import BaseConfig

from . import Executor
from .bootstrap import bootstrap_pipeline


@ray.remote
def ray_execute(message: PipelineMessage) -> PipelineResult:
    return bootstrap_pipeline(message)


class RayExecutor(Executor):
    def __init__(self, config: BaseConfig) -> None:
        options: dict[str, Any] = {
            "local_mode": config.ray.local,
        }
        if config.ray.address:
            options["address"] = config.ray.address
        ray.init(**options)

    async def process_message(self, message: PipelineMessage) -> PipelineResult:
        return await ray_execute.remote(message)
