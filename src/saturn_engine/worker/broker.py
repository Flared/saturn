import asyncio

from saturn_engine.utils.log import getLogger

from .executors.simple import SimpleExecutor
from .scheduler import Scheduler
from .work_manager import WorkManager


class Broker:
    def __init__(self) -> None:
        self.logger = getLogger(__name__, self)
        self.is_running = False
        self.work_manager = WorkManager()
        self.scheduler = Scheduler()
        # TODO: Load executor based on config
        self.executor = SimpleExecutor()

    async def run(self) -> None:
        """
        Start all the task required to run the worker.
        """
        self.is_running = True
        self.logger.info("Starting worker")
        queue_manager_task = self.run_queue_manager()
        worker_manager_task = self.run_worker_manager()
        await asyncio.gather(
            queue_manager_task,
            worker_manager_task,
        )

    async def run_queue_manager(self) -> None:
        """
        Coroutine that keep polling the queues in round-robin and execute their
        pipeline through an executor.
        """
        # Go through all queue in the Ready state.
        async for message in self.scheduler.iter():
            self.logger.debug("Processing message: %s", message)

            # change queue state to Processing
            await self.executor.submit(message)

    async def run_worker_manager(self) -> None:
        """
        Coroutine that periodically sync the queues through the WorkManager.
        This allow to add and remove queues from the scheduler.
        """
        while self.is_running:
            queues_sync = await self.work_manager.sync_queues()
            self.logger.info("Worker sync: %s", queues_sync)

            for queue in queues_sync.add:
                self.scheduler.add(queue)

            for queue in queues_sync.drop:
                self.scheduler.remove(queue)
