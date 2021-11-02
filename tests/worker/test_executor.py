import asyncio
from functools import partial
from typing import Callable

import pytest

from saturn_engine.core import PipelineInfo
from saturn_engine.core.message import PipelineMessage
from saturn_engine.worker.executors import ExecutableMessage
from saturn_engine.worker.executors import Executor
from saturn_engine.worker.executors import ExecutorManager
from saturn_engine.worker.parkers import Parkers
from saturn_engine.worker.resources_manager import ResourceData
from tests.utils import TimeForwardLoop
from tests.worker.conftest import FakeResource


class FakeExecutor(Executor):
    def __init__(self) -> None:
        self.execute_semaphore = asyncio.Semaphore(0)
        self.processing = 0
        self.processed = 0

    async def process_message(self, message: PipelineMessage) -> None:
        self.processing += 1
        await self.execute_semaphore.acquire()
        self.processed += 1


@pytest.mark.asyncio
async def test_base_executor(
    executable_maker: Callable[[], ExecutableMessage],
    event_loop: TimeForwardLoop,
    executor_manager_maker: Callable[..., ExecutorManager],
) -> None:
    executor = FakeExecutor()
    executor_manager = executor_manager_maker(executor=executor)

    for _ in range(10):
        asyncio.create_task(executor_manager.submit(executable_maker()))

    await event_loop.wait_idle()
    assert executor.processing == 5
    assert executor.processed == 0

    for _ in range(10):
        executor.execute_semaphore.release()
    await event_loop.wait_idle()
    assert executor.processed == 10


def pipeline(resource: FakeResource) -> None:
    ...


@pytest.mark.asyncio
async def test_executor_wait_resources_and_queue(
    executable_maker: Callable[..., ExecutableMessage],
    event_loop: TimeForwardLoop,
    executor_manager_maker: Callable[..., ExecutorManager],
) -> None:
    executor = FakeExecutor()
    executor_manager = executor_manager_maker(executor=executor, concurrency=1)
    await executor_manager.resources_manager.add(
        ResourceData(name="r1", type="FakeResource", data={})
    )
    await executor_manager.resources_manager.add(
        ResourceData(name="r2", type="FakeResource", data={})
    )
    parker = Parkers()
    executable_maker = partial(
        executable_maker,
        pipeline_info=PipelineInfo.from_pipeline(pipeline),
        parker=parker,
    )

    # Set up a scenario where there's 2 resource and 1 executor slot.
    # Queuing 3 items should have 1 waiting on the executor and 1 waiting on
    # the resources.
    for _ in range(2):
        await executor_manager.submit(executable_maker())

    await event_loop.wait_idle()
    assert executor.processing == 1
    assert not parker.locked()

    # Submit another task, stuck locking a resource, park the processable.
    await executor_manager.submit(executable_maker())

    await event_loop.wait_idle()
    assert executor.processing == 1
    assert parker.locked()

    # Process the task pending in the executor and release the resource.
    executor.execute_semaphore.release()
    await event_loop.wait_idle()
    assert executor.processed == 1
    assert executor.processing == 2
    assert not parker.locked()

    # Process the other task, release the resource.
    executor.execute_semaphore.release()
    await event_loop.wait_idle()
    assert executor.processed == 2
    assert executor.processing == 3
    assert not parker.locked()

    executor.execute_semaphore.release()
    await event_loop.wait_idle()
