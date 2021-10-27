import asyncstdlib as alib
import pytest

from saturn_engine.core import Message
from saturn_engine.worker.queues.memory import MemoryOptions
from saturn_engine.worker.queues.memory import MemoryPublisher
from saturn_engine.worker.queues.memory import MemoryQueue
from saturn_engine.worker.queues.memory import join_all


@pytest.mark.asyncio
async def test_memory_queues() -> None:
    queue1 = MemoryQueue(MemoryOptions(id="test-1"))
    queue2 = MemoryQueue(MemoryOptions(id="test-2"))
    publisher1 = MemoryPublisher(MemoryOptions(id="test-1"))
    publisher2 = MemoryPublisher(MemoryOptions(id="test-2"))

    queue1generator = queue1.run()
    for i in range(10):
        await publisher1.push(Message(body=f"q1-{i}"))
        await publisher2.push(Message(body=f"q2-{i}"))
        processable = await alib.anext(queue1generator)
        async with processable.process() as message:
            assert message.body == f"q1-{i}"

    queue2generator = queue2.run()
    for i in range(10):
        processable = await alib.anext(queue2generator)
        async with processable.process() as message:
            assert message.body == f"q2-{i}"

    await queue1generator.aclose()
    await queue2generator.aclose()
    await join_all()
