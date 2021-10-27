import asyncio
from typing import Callable
from typing import Optional
from typing import Protocol

from saturn_engine.utils.log import getLogger

from .context import Context
from .executors import Executor
from .executors.simple import SimpleExecutor
from .scheduler import Scheduler
from .services.manager import ServicesManager
from .task_manager import TaskManager
from .work_manager import WorkManager


class WorkManagerInit(Protocol):
    def __call__(self, context: Context) -> WorkManager:
        ...


ExecutorInit = Callable[[], Executor]


class Broker:
    running_task: Optional[asyncio.Future]

    def __init__(
        self,
        *,
        work_manager: WorkManagerInit = WorkManager,
        executor: ExecutorInit = SimpleExecutor
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.is_running = False
        self.running_task = None

        # Build context
        self.services_manager = ServicesManager()
        self.context = Context(services=self.services_manager)

        # Init subsystem
        self.work_manager = work_manager(context=self.context)
        self.task_manager = TaskManager()
        self.scheduler = Scheduler()

        # TODO: Load executor based on config
        self.executor = executor()

    async def run(self) -> None:
        """
        Start all the task required to run the worker.
        """
        self.is_running = True
        self.logger.info("Starting worker")
        self.running_task = asyncio.gather(
            self.run_queue_manager(),
            self.run_worker_manager(),
            self.task_manager.run(),
            self.executor.run(),
        )
        try:
            await self.running_task
        except Exception:
            self.logger.exception("Fatal error in broker")
        except asyncio.CancelledError:
            self.logger.info("Broker was stopped")
        finally:
            self.logger.info("Broker shutting down")
            await self.close()

    async def run_queue_manager(self) -> None:
        """
        Coroutine that keep polling the queues in round-robin and execute their
        pipeline through an executor.
        """
        # Go through all queue in the Ready state.
        async for message in self.scheduler.run():
            self.logger.debug("Processing message: %s", message)
            await self.executor.submit(message)

    async def run_worker_manager(self) -> None:
        """
        Coroutine that periodically sync the queues through the WorkManager.
        This allow to add and remove queues from the scheduler.
        """
        while self.is_running:
            work_sync = await self.work_manager.sync()
            self.logger.info("Worker sync: %s", work_sync)

            for queue in work_sync.queues.add:
                self.scheduler.add(queue)
            for task in work_sync.tasks.add:
                self.task_manager.add(task)

            for queue in work_sync.queues.drop:
                self.scheduler.remove(queue)
            for task in work_sync.tasks.drop:
                self.task_manager.remove(task)

    async def close(self) -> None:
        await self.scheduler.close()
        await self.task_manager.close()
        await self.services_manager.close()
        await self.executor.close()

    def stop(self) -> None:
        if not self.running_task:
            return
        self.running_task.cancel()
