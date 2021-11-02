import asyncio
import sys
from abc import abstractmethod

from saturn_engine.core import PipelineMessage
from saturn_engine.utils.log import getLogger

from ..executable_message import ExecutableMessage


class Executor:
    @abstractmethod
    async def run(self) -> None:
        ...

    @abstractmethod
    async def submit(self, processable: ExecutableMessage) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class BaseExecutor(Executor):
    def __init__(self, concurrency: int = 8) -> None:
        self.logger = getLogger(__name__, self)
        self.concurrency = concurrency
        self.queue: asyncio.Queue[ExecutableMessage] = asyncio.Queue(maxsize=1)
        self.submit_tasks: set[asyncio.Task] = set()
        self.processing_tasks: set[asyncio.Task] = set()

    def start(self) -> None:
        for _ in range(self.concurrency):
            self.processing_tasks.add(asyncio.create_task(self.run()))

    async def run(self) -> None:
        while True:
            processable = await self.queue.get()
            try:
                await self.process_message(processable.message)
            except Exception:
                self.logger.exception("Failed to process: %s", processable)
            finally:
                await processable.message_context.__aexit__(*sys.exc_info())
                await self.release_resources(processable)
                self.queue.task_done()

    async def submit(self, processable: ExecutableMessage) -> None:
        # Try first to check if we have the resources available so we can
        # then check if the executor queue is ready. That way, the scheduler
        # will pause until the executor is free again.
        if await self.acquire_resources(processable, wait=False):
            await self.queue.put(processable)
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
        self.logger.debug("locking resources: %s", missing_resources)
        return True

    async def release_resources(self, processable: ExecutableMessage) -> None:
        return None

    async def delayed_submit(self, processable: ExecutableMessage) -> None:
        """Submit a pipeline after waiting to acquire its resources"""
        await self.acquire_resources(processable, wait=True)
        await processable.unpark()
        await self.queue.put(processable)

    async def close(self) -> None:
        for task in self.submit_tasks:
            task.cancel()

        for task in self.processing_tasks:
            task.cancel()

    @abstractmethod
    async def process_message(self, message: PipelineMessage) -> None:
        ...
