from typing import AsyncContextManager
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import cast

import dataclasses
from contextlib import asynccontextmanager
from datetime import timedelta

import asyncstdlib as alib
import pytest

from saturn_engine.core import TopicMessage
from saturn_engine.core.api import TopicItem
from saturn_engine.worker.services import Services
from saturn_engine.worker.services import ServicesNamespace
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.topic import Topic
from saturn_engine.worker.topic import TopicOutput
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
        context = await scoped_topic_iter.__anext__()
        assert isinstance(context, AsyncContextManager)
        async with context as message:
            ...

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
        context = await scoped_topic_iter.__anext__()
        assert isinstance(context, AsyncContextManager)
        async with context as message:
            ...

    await topic.close()

    assert isinstance(message.args["batch"], list)
    assert len(message.args["batch"]) == 2


class NestedTestTopic(Topic):
    @dataclasses.dataclass
    class Options:
        ...

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        self.entered_context_managers: list[int] = []
        self.exited_context_managers: list[int] = []

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        ...

    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        for i in range(4):
            yield self.message_context(i)

    @asynccontextmanager
    async def message_context(self, value: int) -> AsyncIterator[TopicMessage]:
        try:
            self.entered_context_managers.append(value)
            yield TopicMessage(args={"value": value})
        finally:
            self.exited_context_managers.append(value)


@pytest.mark.asyncio
async def test_batching_topic_context_manager(
    services_manager: ServicesManager,
) -> None:
    topic = BatchingTopic(
        options=BatchingTopic.Options(
            topic=TopicItem(
                name="nested-topic",
                type="tests.worker.topics.test_batching_topic.NestedTestTopic",
                options={},
            ),
            batch_size=2,
        ),
        services=ServicesNamespace(strict=False),
    )

    items = []
    batch_number = 0
    nested_topic = cast(NestedTestTopic, topic.topic)

    async for context in topic.run():
        assert isinstance(context, AsyncContextManager)
        async with context as message:
            items.append(message)

            if batch_number == 0:
                assert sorted(nested_topic.entered_context_managers) == list(range(2))
                assert nested_topic.exited_context_managers == []
            elif batch_number == 1:
                assert sorted(nested_topic.entered_context_managers) == list(range(4))
                assert sorted(nested_topic.exited_context_managers) == list(range(2))

        if batch_number == 0:
            assert sorted(nested_topic.entered_context_managers) == list(range(2))
            assert sorted(nested_topic.exited_context_managers) == list(range(2))
        elif batch_number == 1:
            assert sorted(nested_topic.entered_context_managers) == list(range(4))
            assert sorted(nested_topic.exited_context_managers) == list(range(4))

        batch_number += 1

    assert batch_number == 2
    await topic.close()
