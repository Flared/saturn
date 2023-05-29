import typing as t

from collections.abc import AsyncIterator
from datetime import datetime
from datetime import timedelta
from unittest import mock
from unittest.mock import AsyncMock
from unittest.mock import call

import asyncstdlib as alib
import pytest

from saturn_engine.core import MessageId
from saturn_engine.core import TopicMessage
from saturn_engine.utils import utcnow
from saturn_engine.worker.topics import DelayedTopic
from tests.worker.topics.conftest import RabbitMQTopicMaker

TEST_DELAY: t.Final[int] = 5


async def run_topic(topic: DelayedTopic, length: int) -> AsyncIterator[TopicMessage]:
    async with alib.scoped_iter(topic.run()) as topic_iter:
        async for context in alib.islice(topic_iter, length):
            async with context as message:
                yield message


@pytest.mark.asyncio
async def test_delayed_topic(
    rabbitmq_topic_maker: RabbitMQTopicMaker,
) -> None:
    start_time = utcnow()
    run_times = [start_time, start_time + timedelta(seconds=1)]

    def publish_time() -> datetime:
        return start_time

    def run_time() -> datetime:
        return run_times.pop(0)

    topic = await rabbitmq_topic_maker(DelayedTopic, delay=TEST_DELAY)

    messages = [
        TopicMessage(id=MessageId("a"), args={"message": "test-a"}),
        TopicMessage(id=MessageId("b"), args={"message": "test-b"}),
    ]

    with mock.patch("saturn_engine.worker.topics.delayed.utcnow", new=publish_time):
        for message in messages:
            await topic.publish(message, wait=True)

    with mock.patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        with mock.patch("saturn_engine.worker.topics.delayed.utcnow", new=run_time):
            output = [a async for a in run_topic(topic, 2)]
            assert output == messages
            assert mock_sleep.call_count == 2
            assert mock_sleep.call_args_list == [call(TEST_DELAY), call(TEST_DELAY - 1)]

    await topic.close()
