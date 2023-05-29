import typing as t

import asyncstdlib as alib
import pytest

from saturn_engine.core import MessageId
from saturn_engine.core import TopicMessage
from saturn_engine.worker.topics import PeriodicTopic
from tests.utils import TimeForwardLoop


@pytest.mark.asyncio
async def test_periodic_topic(event_loop: TimeForwardLoop, frozen_time: t.Any) -> None:
    begin_at = event_loop.time()
    topic = PeriodicTopic.from_options({"interval": "*/5 * * * *"})
    async with alib.scoped_iter(topic.run()) as scoped_topic_iter:
        items = await alib.list(alib.islice(scoped_topic_iter, 5))
        assert items == [
            TopicMessage(id=MessageId("1514851500.0"), args={}),
            TopicMessage(id=MessageId("1514851800.0"), args={}),
            TopicMessage(id=MessageId("1514852100.0"), args={}),
            TopicMessage(id=MessageId("1514852400.0"), args={}),
            TopicMessage(id=MessageId("1514852700.0"), args={}),
        ]

    await topic.close()
    assert event_loop.time() - begin_at >= 60 * 5 * 5
