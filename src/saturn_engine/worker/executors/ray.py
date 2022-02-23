from typing import Any

import ray

from saturn_engine.core import PipelineResults
from saturn_engine.worker.pipeline_message import PipelineMessage

from ..services import Services
from . import Executor
from .bootstrap import bootstrap_pipeline
from .bootstrap import wrap_remote_exception


@ray.remote
def ray_execute(message: PipelineMessage) -> PipelineResults:
    with wrap_remote_exception():
        return bootstrap_pipeline(message)


class RayExecutor(Executor):
    def __init__(self, services: Services) -> None:
        options: dict[str, Any] = {
            "local_mode": services.config.c.ray.local,
            "configure_logging": services.config.c.ray.enable_logging,
            "log_to_driver": services.config.c.ray.enable_logging,
        }
        if services.config.c.ray.address:
            options["address"] = services.config.c.ray.address
        ray.init(**options)

    async def process_message(self, message: PipelineMessage) -> PipelineResults:
        return await ray_execute.remote(message)
