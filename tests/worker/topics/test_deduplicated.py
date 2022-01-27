from pathlib import Path
from typing import AsyncContextManager

import asyncstdlib as alib
import pytest

from saturn_engine.core import TopicMessage
from saturn_engine.worker.topics import MemoryTopic
from saturn_engine.worker.topics import DeduplicatedTopic
from saturn_engine.worker.topics.memory import join_all
from saturn_engine.worker.topics.memory import MemoryOptions


@pytest.mark.asyncio
async def test_deduplicated_topic(tmp_path: Path) -> None:
    topic_memory = MemoryTopic(MemoryOptions(name="test_deduplicated_topic"))

    await topic_memory.publish(TopicMessage(args={"id": 1}), wait=True)
    await topic_memory.publish(TopicMessage(args={"id": 2}), wait=True)
    await topic_memory.publish(TopicMessage(args={"id": 2}), wait=True)
    await topic_memory.publish(TopicMessage(args={"id": 2}), wait=True)
    await topic_memory.publish(TopicMessage(args={"id": 3}), wait=True)
    await topic_memory.publish(TopicMessage(args={"id": 4}), wait=True)

    topic_deduplicated = DeduplicatedTopic.from_options(
        {
            "topic": {
                "name": "deduplicated_memory_topic",
                "type": "MemoryTopic",
                "options": {
                    "name": "test_deduplicated_topic",
                }
            }
        },
        services=None,
    )

    topic_deduplicated_generator = topic_deduplicated.run()

    expected_ids = [1, 2, 2, 2, 3, 4]

    for expected_id in expected_ids:
        topic_output = await alib.anext(topic_deduplicated_generator)
        if isinstance(topic_output, AsyncContextManager):
            async with topic_output as m:
                message = m
                print("salut toi")
        else:
            message = topic_output
        assert message.args["id"] == expected_id

    await topic_deduplicated_generator.aclose()
    await join_all()

