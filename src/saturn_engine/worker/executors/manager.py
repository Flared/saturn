import asyncio

from saturn_engine.core.api import ExecutorItem
from saturn_engine.utils.asyncutils import TasksGroupRunner
from saturn_engine.worker.services import Services

from ..resources_manager import ResourcesManager
from . import Executor
from . import get_executor_class
from .executable import ExecutableMessage
from .executable import ExecutableQueue
from .queue import ExecutorQueue
from .scheduler import Scheduler


class ExecutorsManager:
    def __init__(
        self,
        *,
        resources_manager: ResourcesManager,
        services: Services,
    ) -> None:
        self.resources_manager = resources_manager
        self.services = services
        self.executors: dict[str, ExecutorWorker] = {}
        self.executors_tasks_group = TasksGroupRunner(name="executors")

        # TODO: Executor should be loaded through definitions and
        # `add_executor`.
        self.add_executor(
            ExecutorItem(
                name="default",
                type=services.s.config.c.worker.executor_cls,
            ),
        )

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

    def add_executor(self, executor_definition: ExecutorItem) -> None:
        if executor_definition.name in self.executors:
            raise ValueError("Executor already defined")

        executor = ExecutorWorker.from_item(
            executor_definition,
            services=self.services,
            resources_manager=self.resources_manager,
        )
        name = executor_definition.name
        self.executors[name] = executor
        self.executors_tasks_group.add(
            asyncio.create_task(executor.run(), name=f"executor-worker({name})")
        )


class ExecutorWorker:
    def __init__(
        self,
        *,
        executor: Executor,
        resources_manager: ResourcesManager,
        services: Services,
    ) -> None:
        self.services = services
        self.executor_queue = ExecutorQueue(
            resources_manager=resources_manager,
            executor=executor,
            services=services,
        )
        self.scheduler: Scheduler[ExecutableMessage] = Scheduler()

    @classmethod
    def from_item(
        cls,
        executor_definition: ExecutorItem,
        *,
        resources_manager: ResourcesManager,
        services: Services,
    ) -> "ExecutorWorker":
        executor_cls = get_executor_class(executor_definition.type)
        executor = executor_cls(services=services)
        return cls(
            executor=executor,
            resources_manager=resources_manager,
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
