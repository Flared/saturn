import asyncio
from typing import Callable

import pytest

from saturn_engine.core.message import PipelineMessage
from saturn_engine.worker.executors import BaseExecutor
from saturn_engine.worker.executors import ExecutableMessage
from saturn_engine.worker.queues import Parkers
from tests.utils import TimeForwardLoop


class FakeExecutor(BaseExecutor):
    def __init__(self, concurrency: int = 5, resources_count: int = 2 ** 16) -> None:
        super().__init__(concurrency=concurrency)
        self.execute_semaphore = asyncio.Semaphore(0)
        self.processing = 0
        self.processed = 0
        self.resources_semaphore = asyncio.Semaphore(resources_count)

    async def process_message(self, message: PipelineMessage) -> None:
        self.processing += 1
        await self.execute_semaphore.acquire()
        self.processed += 1

    async def acquire_resources(
        self, processable: ExecutableMessage, *, wait: bool
    ) -> bool:
        if not self.resources_semaphore:
            return True

        if not wait and self.resources_semaphore.locked():
            return False

        await self.resources_semaphore.acquire()
        return True

    async def release_resources(self, processable: ExecutableMessage) -> None:
        if self.resources_semaphore:
            self.resources_semaphore.release()


@pytest.mark.asyncio
async def test_base_executor(
    executable_maker: Callable[[], ExecutableMessage], event_loop: TimeForwardLoop
) -> None:

    executor = FakeExecutor()
    executor.start()

    for _ in range(10):
        asyncio.create_task(executor.submit(executable_maker()))

    await event_loop.wait_idle()
    assert executor.processing == 5
    assert executor.processed == 0

    for _ in range(10):
        executor.execute_semaphore.release()
    await event_loop.wait_idle()
    assert executor.processed == 10

    await executor.close()


@pytest.mark.asyncio
async def test_executor_wait_resources_and_queue(
    executable_maker: Callable[..., ExecutableMessage], event_loop: TimeForwardLoop
) -> None:
    executor = FakeExecutor(concurrency=1, resources_count=2)
    executor.start()
    parker = Parkers()

    # Set up a scenario where there's 2 resource and 1 executor slot.
    # Queuing 3 items should have 1 waiting on the executor and 1 waiting on
    # the resources.
    for _ in range(2):
        await executor.submit(executable_maker(parker=parker))

    await event_loop.wait_idle()
    assert executor.processing == 1
    assert executor.resources_semaphore.locked()
    assert not parker.locked()

    # Submit another task, stuck locking a resource, park the processable.
    await executor.submit(executable_maker(parker=parker))

    await event_loop.wait_idle()
    assert executor.processing == 1
    assert executor.resources_semaphore.locked()
    assert parker.locked()

    # Process the task pending in the executor and release the resource.
    executor.execute_semaphore.release()
    await event_loop.wait_idle()
    assert executor.processed == 1
    assert executor.processing == 2
    assert executor.resources_semaphore.locked()
    assert not parker.locked()

    # Process the other task, release the resource.
    executor.execute_semaphore.release()
    await event_loop.wait_idle()
    assert executor.processed == 2
    assert executor.processing == 3
    assert not executor.resources_semaphore.locked()
    assert not parker.locked()

    executor.execute_semaphore.release()
    await event_loop.wait_idle()

    await executor.close()
