import asyncio

from saturn_engine.core import api
from saturn_engine.utils.asyncutils import TasksGroupRunner
from saturn_engine.worker.services import Services

from . import Executor
from . import build_executor
from .executable import ExecutableMessage
from .executable import ExecutableQueue
from .queue import ExecutorQueue
from .scheduler import Scheduler


class ExecutorsManager:
    def __init__(
        self,
        *,
        services: Services,
    ) -> None:
        self.services = services
        self.executors: dict[str, ExecutorWorker] = {}
        self.executors_tasks_group = TasksGroupRunner(name="executors")

    def start(self) -> None:
        self.executors_tasks_group.start()

    async def close(self) -> None:
        await asyncio.gather(
            *[executor.close() for executor in self.executors.values()]
        )
        await self.executors_tasks_group.close()

    def add_queue(self, queue: ExecutableQueue) -> None:
        executor = self.executors.get(queue.executor)
        if not executor:
            raise ValueError("Executor missing")
        executor.add_schedulable(queue)

    async def remove_queue(self, queue: ExecutableQueue) -> None:
        executor = self.executors.get(queue.executor)
        if not executor:
            return
        await executor.remove_schedulable(queue)

    def add_executor(self, executor_definition: api.Executor) -> None:
        if executor_definition.name in self.executors:
            raise ValueError("Executor already defined")

        executor = ExecutorWorker.from_item(
            executor_definition,
            services=self.services,
        )
        name = executor_definition.name
        self.executors[name] = executor
        self.executors_tasks_group.add(
            asyncio.create_task(executor.run(), name=f"executor-worker({name})")
        )

    async def remove_executor(self, executor_definition: api.Executor) -> None:
        executor = self.executors.pop(executor_definition.name, None)
        if not executor:
            raise ValueError("Executor missing")
        await executor.close()


class ExecutorWorker:
    def __init__(
        self,
        *,
        executor: Executor,
        services: Services,
    ) -> None:
        self.services = services
        self.executor_queue = ExecutorQueue(
            executor=executor,
            services=services,
        )
        self.scheduler: Scheduler[ExecutableMessage] = Scheduler()

    @classmethod
    def from_item(
        cls,
        executor_definition: api.Executor,
        *,
        services: Services,
    ) -> "ExecutorWorker":
        executor = build_executor(executor_definition, services=services)
        return cls(
            executor=executor,
            services=services,
        )

    async def run(self) -> None:
        """
        Coroutine that keep polling the queues in round-robin and execute their
        pipeline through an executor.
        """
        self.executor_queue.start()

        # Go through all queue in the Ready state.
        async for message in self.scheduler.run():
            await self.services.s.hooks.message_scheduled.emit(message.message)
            await self.executor_queue.submit(message)

    async def close(self) -> None:
        await self.scheduler.close()
        await self.executor_queue.close()

    def add_schedulable(self, schedulable: ExecutableQueue) -> None:
        self.scheduler.add(schedulable)

    async def remove_schedulable(self, schedulable: ExecutableQueue) -> None:
        await self.scheduler.remove(schedulable)
