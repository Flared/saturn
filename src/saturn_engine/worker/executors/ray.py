import typing as t

import contextlib
from collections.abc import Iterator

import ray

from saturn_engine.core import PipelineResults
from saturn_engine.utils.hooks import EventHook
from saturn_engine.utils.log import getLogger
from saturn_engine.utils.ray import ActorPool
from saturn_engine.worker.pipeline_message import PipelineMessage

from ..services import Services
from . import Executor
from .bootstrap import PipelineBootstrap
from .bootstrap import wrap_remote_exception


# Set max_restarts to 0 since the ActorPool take care of popping new actor in
# case of failure.
@ray.remote(max_restarts=0)  # type: ignore
class SaturnExecutorActor:
    def __init__(self, executor_initialized: EventHook[PipelineBootstrap]):
        self.bootstrapper = PipelineBootstrap(executor_initialized)

    def process_message(self, message: PipelineMessage) -> PipelineResults:
        with wrap_remote_exception():
            return self.bootstrapper.bootstrap_pipeline(message)


class ExecutorSession:
    def __init__(self, services: Services) -> None:
        config = services.config.c.ray
        options: dict[str, t.Any] = {
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

    @staticmethod
    def concurrency(services: Services) -> int:
        config = services.config.c.ray
        return config.executor_actor_concurrency * config.executor_actor_count

    def close(self) -> None:
        pass


class RayExecutor(Executor):
    def __init__(self, services: Services) -> None:
        self.logger = getLogger(__name__, self)
        self.services = services
        self._session: t.Optional[ExecutorSession] = None

    @property
    def concurrency(self) -> int:
        return ExecutorSession.concurrency(self.services)

    @contextlib.contextmanager
    def session(self) -> Iterator[ExecutorSession]:
        need_reconnect = False
        try:
            if not self._session:
                self._session = ExecutorSession(self.services)
            yield self._session
        except ConnectionError:
            self.logger.error("Could not reconnect")
            need_reconnect = True
            raise
        except Exception as e:
            if "Ray Client is not connected" in str(e):
                self.logger.error("Lost connection")
                need_reconnect = True
            raise
        finally:
            if need_reconnect:
                if self._session:
                    self._session.close()
                self._session = None

    async def process_message(self, message: PipelineMessage) -> PipelineResults:
        with self.session() as session:
            async with session.pool.scoped_actor() as actor:
                return await actor.process_message.remote(message)
