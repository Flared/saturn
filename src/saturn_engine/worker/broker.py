from typing import Optional
from typing import Protocol

import asyncio

from saturn_engine.config import Config
from saturn_engine.utils.log import getLogger

from .executors.manager import ExecutorsManager
from .services import Services
from .services.manager import ServicesManager
from .work_manager import WorkManager


class WorkManagerInit(Protocol):
    def __call__(self, services: Services) -> WorkManager:
        ...


class Broker:
    running_task: Optional[asyncio.Future]
    work_manager: WorkManager

    def __init__(
        self,
        config: Config,
        *,
        work_manager: WorkManagerInit = WorkManager,
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.is_running = False
        self.is_init = False
        self.running_task = None

        self.services_manager = ServicesManager(config=config)
        self.work_manager_init = work_manager
        self.resources_manager = self.services_manager.services.s.resources_manager

    async def init(self) -> None:
        if self.is_init:
            return

        await self.services_manager.open()
        self.work_manager = self.work_manager_init(
            services=self.services_manager.services
        )

        self.executors_manager = ExecutorsManager(
            services=self.services_manager.services,
        )

        self.is_init = True

    async def run(self) -> None:
        """
        Start all the task required to run the worker.
        """
        self.is_running = True
        self.logger.info("Initializing worker")
        await self.init()
        self.logger.info("Starting worker")
        self.executors_manager.start()
        self.running_task = asyncio.create_task(self.run_sync(), name="broker.sync")
        try:
            await self.running_task
        except Exception:
            self.logger.exception("Fatal error in broker")
        except asyncio.CancelledError:
            self.logger.info("Broker was stopped")
        finally:
            self.logger.info("Broker shutting down")
            await self.close()

    async def run_sync(self) -> None:
        """
        Coroutine that periodically sync the queues through the WorkManager.
        This allow to add and remove queues from the scheduler.
        """
        while self.is_running:
            work_sync = await self.work_manager.sync()
            self.logger.info("Worker sync", extra={"data": {"work": str(work_sync)}})

            for executor in work_sync.executors.add:
                self.executors_manager.add_executor(executor)
            for queue in work_sync.queues.add:
                self.executors_manager.add_queue(queue)
            for resource in work_sync.resources.add:
                await self.resources_manager.add(resource)
            for resources_provider in work_sync.resources_providers.add:
                await resources_provider.open()

            for resources_provider in work_sync.resources_providers.drop:
                await resources_provider.close()
            for resource in work_sync.resources.drop:
                await self.resources_manager.remove(resource.key)
            for queue in work_sync.queues.drop:
                await self.executors_manager.remove_queue(queue)
            for executor in work_sync.executors.drop:
                await self.executors_manager.remove_executor(executor)

    async def close(self) -> None:
        await self.executors_manager.close()
        await self.services_manager.close()

    def stop(self) -> None:
        self.logger.info("Stopping broker")
        if not self.running_task:
            return
        self.running_task.cancel()
