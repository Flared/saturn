import asyncio
import threading
from typing import Optional

import asyncstdlib as alib
import pytest

from saturn_engine.core import TopicMessage
from saturn_engine.worker.topics import BlockingTopic
from tests.utils import TimeForwardLoop


@pytest.mark.asyncio
async def test_blocking_topic(event_loop: TimeForwardLoop) -> None:
    event = threading.Event()

    class FakeTopic(BlockingTopic):
        def __init__(self) -> None:
            super().__init__()
            self.published: list[str] = []
            self.x = 0

        def run_once_blocking(self) -> Optional[TopicMessage]:
            self.x += 1
            if self.x == 3:
                return None
            return TopicMessage(id=str(self.x), args={})

        def publish_blocking(self, message: TopicMessage, wait: bool) -> bool:
            if message.args["block"]:
                if wait:
                    event.wait()
                else:
                    return False
            self.published.append(message.id)
            return True

    topic = FakeTopic()

    assert await alib.list(topic.run()) == [
        TopicMessage(id="1", args={}),
        TopicMessage(id="2", args={}),
    ]

    assert await topic.publish(TopicMessage(id="1", args={"block": False}), wait=True)
    assert await topic.publish(TopicMessage(id="2", args={"block": False}), wait=False)
    assert not await topic.publish(
        TopicMessage(id="3", args={"block": True}), wait=False
    )

    async with event_loop.until_idle():
        publish_task1 = asyncio.create_task(
            topic.publish(TopicMessage(id="4", args={"block": True}), wait=True)
        )

    assert not await topic.publish(
        TopicMessage(id="5", args={"block": False}), wait=False
    )
    publish_task2 = asyncio.create_task(
        topic.publish(TopicMessage(id="6", args={"block": False}), wait=True)
    )

    event.set()
    assert await publish_task1
    assert await publish_task2

    assert topic.published == ["1", "2", "4", "6"]
