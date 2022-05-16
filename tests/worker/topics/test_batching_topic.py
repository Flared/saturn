from datetime import timedelta

import asyncstdlib as alib
import pytest

from saturn_engine.core.api import TopicItem
from saturn_engine.worker.services import ServicesNamespace
from saturn_engine.worker.topics.batching import BatchingTopic


@pytest.mark.asyncio
async def test_batching_topic_batch_size() -> None:
    BATCH_SIZE = 7

    topic = BatchingTopic(
        options=BatchingTopic.Options(
            topic=TopicItem(
                name="static-topic-with-infinite-messages",
                type="StaticTopic",
                options={
                    "messages": [{"args": {}}],
                    "cycle": True,
                },
            ),
            batch_size=BATCH_SIZE,
        ),
        services=ServicesNamespace(strict=False),
    )

    async with alib.scoped_iter(topic.run()) as scoped_topic_iter:
        message = await scoped_topic_iter.__anext__()
    await topic.close()

    assert isinstance(message.args["batch"], list)
    assert len(message.args["batch"]) == BATCH_SIZE


@pytest.mark.asyncio
async def test_batching_topic_flush_timeout() -> None:
    FLUSH_TIMEOUT = timedelta(seconds=10)

    topic = BatchingTopic(
        options=BatchingTopic.Options(
            topic=TopicItem(
                name="periodic-topic",
                type="PeriodicTopic",
                options={
                    "interval": "* * * * * */4",
                },
            ),
            flush_timeout=FLUSH_TIMEOUT,
        ),
        services=ServicesNamespace(strict=False),
    )

    async with alib.scoped_iter(topic.run()) as scoped_topic_iter:
        message = await scoped_topic_iter.__anext__()

    await topic.close()

    assert isinstance(message.args["batch"], list)
    assert len(message.args["batch"]) == 2
