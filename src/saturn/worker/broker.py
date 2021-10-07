import asyncio

from .executors.simple import SimpleExecutor
from .queue import Queue
from .scheduler import Scheduler
from .work_manager import WorkManager


class Broker:
    def __init__(self) -> None:
        self.is_running = False
        self.work_manager = WorkManager()
        self.scheduler: Scheduler[Queue] = Scheduler()
        # TODO: Load executor based on config
        self.executor = SimpleExecutor()

    async def run(self) -> None:
        """
        Start all the task required to run the worker.
        """
        self.is_running = True
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
        while self.is_running:
            # Go through all queue in the Ready state.
            for queue in self.scheduler.iter_ready():
                message = queue.get_nowait()
                if message is None:
                    # TODO: log warning if message is None. It wasn't ready.
                    continue

                # change queue state to Processing
                self.scheduler.set_processing(queue)
                await self.executor.submit(message)

            # Check queue in Waiting state that must go to Ready state.
            await self.scheduler.wait_ready()

    async def run_worker_manager(self) -> None:
        """
        Coroutine that periodically sync the queues through the WorkManager.
        This allow to add and remove queues from the scheduler.
        """
        while self.is_running:
            queues_sync = await self.work_manager.sync_queues()

            for queue in queues_sync.add:
                self.scheduler.add(queue)

            for queue in queues_sync.drop:
                self.scheduler.remove(queue)
