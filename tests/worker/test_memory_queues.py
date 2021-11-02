import asyncstdlib as alib
import pytest

from saturn_engine.core import TopicMessage
from saturn_engine.worker.topics.memory import MemoryOptions
from saturn_engine.worker.topics.memory import MemoryTopic
from saturn_engine.worker.topics.memory import join_all


@pytest.mark.asyncio
async def test_memory_queues() -> None:
    queue1 = MemoryTopic(MemoryOptions(name="test-1"))
    queue2 = MemoryTopic(MemoryOptions(name="test-2"))
    publisher1 = MemoryTopic(MemoryOptions(name="test-1"))
    publisher2 = MemoryTopic(MemoryOptions(name="test-2"))

    queue1generator = queue1.run()
    for i in range(10):
        await publisher1.push(TopicMessage(args={"id": i}))
        await publisher2.push(TopicMessage(args={"id": i}))
        processable = await alib.anext(queue1generator)
        async with processable as message:
            assert message.args["id"] == i

    queue2generator = queue2.run()
    for i in range(10):
        processable = await alib.anext(queue2generator)
        async with processable as message:
            assert message.args["id"] == i

    await queue1generator.aclose()
    await queue2generator.aclose()
    await join_all()
