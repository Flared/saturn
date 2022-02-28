from typing import Any

import ray

from saturn_engine.core import PipelineResults
from saturn_engine.utils.hooks import EventHook
from saturn_engine.utils.ray import ActorPool
from saturn_engine.worker.pipeline_message import PipelineMessage

from ..services import Services
from . import Executor
from .bootstrap import PipelineBootstrap
from .bootstrap import wrap_remote_exception


@ray.remote
class SaturnExecutorActor:
    def __init__(self, executor_initialized: EventHook[PipelineBootstrap]):
        self.bootstrapper = PipelineBootstrap(executor_initialized)

    def process_message(self, message: PipelineMessage) -> PipelineResults:
        with wrap_remote_exception():
            return self.bootstrapper.bootstrap_pipeline(message)


class RayExecutor(Executor):
    def __init__(self, services: Services) -> None:
        config = services.config.c.ray
        options: dict[str, Any] = {
            "local_mode": config.local,
            "configure_logging": config.enable_logging,
            "log_to_driver": config.enable_logging,
        }
        if config.address:
            options["address"] = config.address
        ray.init(**options)

        self.pool = ActorPool(
            count=config.executor_actor_count,
            actor_cls=SaturnExecutorActor,
            actor_options={
                "max_concurrency": config.executor_actor_concurrency,
                "num_cpus": config.executor_actor_cpu_count,
            },
            actor_kwargs={
                "executor_initialized": services.hooks.executor_initialized,
            },
        )

    async def process_message(self, message: PipelineMessage) -> PipelineResults:
        async with self.pool.scoped_actor() as actor:
            return await actor.process_message.remote(message)
