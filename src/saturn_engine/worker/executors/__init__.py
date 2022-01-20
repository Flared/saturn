from typing import Type

import asyncio
import contextlib
from abc import abstractmethod

from saturn_engine.core import PipelineOutput
from saturn_engine.core import PipelineResult
from saturn_engine.utils.inspect import import_name
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.hooks import MessagePublished

from ..executable_message import ExecutableMessage
from ..resources_manager import ResourcesManager
from ..resources_manager import ResourceUnavailable


class Executor:
    def __init__(self, services: Services) -> None:
        pass

    @abstractmethod
    async def process_message(self, message: PipelineMessage) -> PipelineResult:
        ...

    async def close(self) -> None:
        pass


def get_executor_class(path: str) -> Type[Executor]:
    klass = BUILTINS.get(path)
    if klass:
        return klass

    return import_name(path)


class ExecutorManager:
    def __init__(
        self,
        resources_manager: ResourcesManager,
        executor: Executor,
        services: Services,
        concurrency: int = 8,
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.concurrency = concurrency
        self.queue: asyncio.Queue[ExecutableMessage] = asyncio.Queue(maxsize=1)
        self.submit_tasks: set[asyncio.Task] = set()
        self.processing_tasks: set[asyncio.Task] = set()
        self.message_executor = executor
        self.resources_manager = resources_manager
        self.executor = executor
        self.services = services

    def start(self) -> None:
        for _ in range(self.concurrency):
            self.logger.debug("Spawning new queue task")
            self.processing_tasks.add(asyncio.create_task(self.run_queue()))

    async def run_queue(self) -> None:
        while True:
            processable = await self.queue.get()
            processable.context.callback(self.queue.task_done)
            try:
                async with processable.context:

                    @self.services.hooks.message_executed.emit
                    async def scope(message: PipelineMessage) -> PipelineResult:
                        return await self.executor.process_message(message)

                    with contextlib.suppress(Exception):
                        output = await scope(processable.message)

                    processable.update_resources_used(output.resources)
                    asyncio.create_task(
                        self.consume_output(
                            processable=processable, output=output.outputs
                        )
                    )
            except Exception:
                self.logger.exception("Failed to process queue item")

    async def submit(self, processable: ExecutableMessage) -> None:
        # Try first to check if we have the resources available so we can
        # then check if the executor queue is ready. That way, the scheduler
        # will pause until the executor is free again.
        if await self.acquire_resources(processable, wait=False):
            await self.queue_submit(processable)
        else:
            # Park the queue from which the processable comes from.
            # The queue should be unparked once the resources are acquired.
            processable.park()
            # To avoid blocking the executor queue while we wait on resource,
            # create a background task to wait on resources.
            self.submit_tasks.add(asyncio.create_task(self.delayed_submit(processable)))

    async def acquire_resources(
        self, processable: ExecutableMessage, *, wait: bool
    ) -> bool:
        missing_resources = processable.message.missing_resources
        if not missing_resources:
            return True

        self.logger.debug("locking resources: %s", missing_resources)
        try:
            resources_context = await self.resources_manager.acquire_many(
                missing_resources, wait=wait
            )
        except ResourceUnavailable:
            return False

        resources = await processable.attach_resources(resources_context)
        self.logger.debug("locked resources: %s", resources)
        return True

    async def delayed_submit(self, processable: ExecutableMessage) -> None:
        """Submit a pipeline after waiting to acquire its resources"""
        try:
            await self.acquire_resources(processable, wait=True)
        finally:
            await processable.unpark()

        await self.queue_submit(processable)

    async def queue_submit(self, processable: ExecutableMessage) -> None:
        await self.services.s.hooks.message_submitted.emit(processable.message)
        await self.queue.put(processable)

    async def consume_output(
        self, *, processable: ExecutableMessage, output: list[PipelineOutput]
    ) -> None:
        try:
            for item in output:
                topics = processable.output.get(item.channel, [])
                for topic in topics:

                    @self.services.s.hooks.message_published.emit
                    async def scope(_: MessagePublished) -> None:
                        if topic is None:
                            return
                        if await topic.publish(item.message, wait=False):
                            return
                        processable.park()
                        await topic.publish(item.message, wait=True)

                    with contextlib.suppress(Exception):
                        await scope(
                            MessagePublished(
                                message=processable.message, topic=topic, output=item
                            )
                        )
        finally:
            await processable.unpark()

    async def close(self) -> None:
        for task in self.submit_tasks:
            task.cancel()

        for task in self.processing_tasks:
            task.cancel()


from .process import ProcessExecutor

BUILTINS: dict[str, Type[Executor]] = {
    "ProcessExecutor": ProcessExecutor,
}

try:
    from .ray import RayExecutor

    BUILTINS["RayExecutor"] = RayExecutor
except ImportError:
    pass
