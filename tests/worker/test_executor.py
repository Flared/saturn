from typing import Callable

import asyncio
from functools import partial

import pytest

from saturn_engine.core import PipelineInfo
from saturn_engine.core import PipelineOutput
from saturn_engine.core import PipelineResults
from saturn_engine.core import TopicMessage
from saturn_engine.core.api import TopicItem
from saturn_engine.worker.executors import Executor
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.executors.parkers import Parkers
from saturn_engine.worker.executors.queue import ExecutorQueue
from saturn_engine.worker.resources.manager import ResourceData
from saturn_engine.worker.topics.memory import MemoryTopic
from saturn_engine.worker.topics.memory import get_queue
from tests.utils import TimeForwardLoop
from tests.worker.conftest import FakeResource


class FakeExecutor(Executor):
    concurrency = 1

    def __init__(self) -> None:
        self.execute_semaphore = asyncio.Semaphore(0)
        self.processing = 0
        self.processed = 0

    async def process_message(self, message: ExecutableMessage) -> PipelineResults:
        self.processing += 1
        await self.execute_semaphore.acquire()
        self.processed += 1
        return PipelineResults(
            outputs=[
                PipelineOutput(
                    channel="default", message=TopicMessage(args={"n": self.processed})
                )
            ],
            resources=[],
        )


class FakeFailingExecutor(FakeExecutor):
    async def process_message(self, message: ExecutableMessage) -> PipelineResults:
        self.processed += 1
        raise Exception("TEST_EXCEPTION")


@pytest.mark.asyncio
async def test_base_executor(
    executable_maker: Callable[[], ExecutableMessage],
    event_loop: TimeForwardLoop,
    executor_queue_maker: Callable[..., ExecutorQueue],
) -> None:
    executor = FakeExecutor()
    executor.concurrency = 5
    executor_manager = executor_queue_maker(executor=executor)

    async with event_loop.until_idle():
        for _ in range(10):
            asyncio.create_task(executor_manager.submit(executable_maker()))

    assert executor.processing == 5
    assert executor.processed == 0

    async with event_loop.until_idle():
        for _ in range(10):
            executor.execute_semaphore.release()
    assert executor.processed == 10


def pipeline(resource: FakeResource) -> None:
    ...


@pytest.mark.asyncio
async def test_executor_wait_resources_and_queue(
    executable_maker: Callable[..., ExecutableMessage],
    event_loop: TimeForwardLoop,
    executor_queue_maker: Callable[..., ExecutorQueue],
) -> None:
    executor = FakeExecutor()
    executor_manager = executor_queue_maker(executor=executor)
    await executor_manager.resources_manager.add(
        ResourceData(name="r1", type=FakeResource._typename(), data={})
    )
    await executor_manager.resources_manager.add(
        ResourceData(name="r2", type=FakeResource._typename(), data={})
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
    async with event_loop.until_idle():
        for _ in range(2):
            await executor_manager.submit(executable_maker())

    assert executor.processing == 1
    assert not parker.locked()

    # Submit another task, stuck locking a resource, park the processable.
    async with event_loop.until_idle():
        await executor_manager.submit(executable_maker())

    assert executor.processing == 1
    assert parker.locked()

    # Process the task pending in the executor and release the resource.
    async with event_loop.until_idle():
        executor.execute_semaphore.release()
    assert executor.processed == 1
    assert executor.processing == 2
    assert not parker.locked()

    # Process the other task, release the resource.
    async with event_loop.until_idle():
        executor.execute_semaphore.release()
    assert executor.processed == 2
    assert executor.processing == 3
    assert not parker.locked()

    async with event_loop.until_idle():
        executor.execute_semaphore.release()


@pytest.mark.asyncio
async def test_executor_wait_pusblish_and_queue(
    executable_maker: Callable[..., ExecutableMessage],
    event_loop: TimeForwardLoop,
    executor_queue_maker: Callable[..., ExecutorQueue],
) -> None:
    executor = FakeExecutor()
    executor_manager = executor_queue_maker(executor=executor)
    await executor_manager.resources_manager.add(
        ResourceData(name="r1", type=FakeResource._typename(), data={})
    )
    await executor_manager.resources_manager.add(
        ResourceData(name="r2", type=FakeResource._typename(), data={})
    )
    output_queue = get_queue("q1", maxsize=1)
    output_topic = MemoryTopic(MemoryTopic.Options(name="q1"))
    parker = Parkers()
    executable_maker = partial(
        executable_maker,
        pipeline_info=PipelineInfo.from_pipeline(pipeline),
        parker=parker,
        output={"default": [output_topic]},
    )

    # Set up a scenario where there's 2 task, 1 executor slot and 1 publish slot.
    # Queuing 2 items should have 1 waiting on the executor and 1 waiting on publish
    # the resources.
    async with event_loop.until_idle():
        for _ in range(2):
            await executor_manager.submit(executable_maker())

    assert executor.processing == 1
    assert executor.processed == 0
    assert output_queue.qsize() == 0
    assert not parker.locked()

    # Process one task, take publish slot.
    async with event_loop.until_idle():
        executor.execute_semaphore.release()

    assert executor.processing == 2
    assert executor.processed == 1
    assert output_queue.qsize() == 1
    assert not parker.locked()

    # Process the other task, get stuck on publishing
    async with event_loop.until_idle():
        executor.execute_semaphore.release()

    assert executor.processing == 2
    assert executor.processed == 2
    assert output_queue.qsize() == 1
    assert parker.locked()

    # Pop the item in the publish queue, leaving room for the next item.
    async with event_loop.until_idle():
        assert output_queue.get_nowait().args == {"n": 1}

    assert executor.processing == 2
    assert executor.processed == 2
    assert output_queue.qsize() == 1
    assert not parker.locked()

    # Pop the other item in the publish queue, clearing the queue.
    async with event_loop.until_idle():
        assert output_queue.get_nowait().args == {"n": 2}

    assert executor.processing == 2
    assert executor.processed == 2
    assert output_queue.qsize() == 0
    assert not parker.locked()


@pytest.mark.asyncio
async def test_executor_error_handler(
    fake_executable_maker_with_output: Callable[..., ExecutableMessage],
    event_loop: TimeForwardLoop,
    executor_queue_maker: Callable[..., ExecutorQueue],
) -> None:
    executor = FakeFailingExecutor()
    executor.concurrency = 1
    executor_manager = executor_queue_maker(executor=executor)
    output_queue = get_queue("q1", maxsize=1)
    output_topics = {
        "error:TEST_EXCEPTION:Exception": [
            TopicItem(
                "q1",
                "MemoryTopic",
            )
        ]
    }
    # Execute our failing message
    async with event_loop.until_idle():
        asyncio.create_task(
            executor_manager.submit(
                fake_executable_maker_with_output(
                    output=output_topics,
                )
            )
        )

    # Our pipeline should cause a test exception and publish it in its channel
    assert output_queue.qsize() == 1
    output: TopicMessage = output_queue.get_nowait()

    # We can't really assert the traceback and id fields
    # so we just copy them from our output
    expected_message = TopicMessage(
        id=output.id,
        args={
            "type": "Exception",
            "module": "tests.worker.test_executor",
            "message": "TEST_EXCEPTION",
            "traceback": output.args["traceback"],
        },
        config={},
        metadata={},
        tags={},
    )

    assert expected_message == output
