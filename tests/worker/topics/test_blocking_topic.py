from typing import Optional

import asyncio
import threading

import asyncstdlib as alib
import pytest

from saturn_engine.core import TopicMessage
from saturn_engine.worker.topic import TopicOutput
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

        def run_once_blocking(self) -> Optional[list[TopicOutput]]:
            if self.x == 0:
                self.x += 2
                return [
                    TopicMessage(id=str(1), args={}),
                    TopicMessage(id=str(2), args={}),
                ]

            self.x += 1
            if self.x == 3:
                return None
            return [TopicMessage(id=str(self.x), args={})]

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


@pytest.mark.asyncio
async def test_blocking_topic_error(event_loop: TimeForwardLoop) -> None:
    class FakeTopic(BlockingTopic):
        def __init__(self) -> None:
            super().__init__()
            self.items = ["1", ValueError(), "2", ValueError()]

        def run_once_blocking(self) -> Optional[list[TopicOutput]]:
            if not self.items:
                return None
            item = self.items.pop(0)
            if isinstance(item, Exception):
                raise item
            return [TopicMessage(id=str(item), args={})]

    topic = FakeTopic()
    assert await alib.list(topic.run()) == [
        TopicMessage(id="1", args={}),
        TopicMessage(id="2", args={}),
    ]
