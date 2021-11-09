from typing import Optional

import asyncstdlib as alib
import pytest

from saturn_engine.core import TopicMessage
from saturn_engine.worker.topics import BlockingTopic


@pytest.mark.asyncio
async def test_blocking_topic() -> None:
    class FakeTopic(BlockingTopic):
        def __init__(self) -> None:
            self.published: list[tuple[TopicMessage, bool]] = []
            self.x = 0

        def run_once_blocking(self) -> Optional[TopicMessage]:
            self.x += 1
            if self.x == 3:
                return None
            return TopicMessage(id=str(self.x), args={})

        def publish_blocking(self, message: TopicMessage, wait: bool) -> bool:
            self.published.append((message, wait))
            return wait

    topic = FakeTopic()

    assert await alib.list(topic.run()) == [
        TopicMessage(id="1", args={}),
        TopicMessage(id="2", args={}),
    ]
    assert await topic.publish(TopicMessage(id="1", args={}), wait=True)
    assert not await topic.publish(TopicMessage(id="2", args={}), wait=False)
    assert topic.published == [
        (TopicMessage(id="1", args={}), True),
        (TopicMessage(id="2", args={}), False),
    ]
