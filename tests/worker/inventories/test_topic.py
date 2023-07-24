import typing as t

from contextlib import asynccontextmanager

import asyncstdlib as alib
import pytest

from saturn_engine.core.topic import TopicMessage
from saturn_engine.utils.inspect import get_import_name
from saturn_engine.worker.inventories.topic import TopicAdapter
from saturn_engine.worker.topic import Topic
from saturn_engine.worker.topic import TopicOutput


class FakeTopic(Topic):
    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        self.processing = 0
        self.processed = 0

    async def run(self) -> t.AsyncGenerator[TopicOutput, None]:
        for x in range(10):
            yield self.context(TopicMessage(args={"x": x}))

    @asynccontextmanager
    async def context(self, message: TopicMessage) -> t.AsyncIterator[TopicMessage]:
        self.processing += 1
        yield message
        self.processed += 1


@pytest.mark.asyncio
async def test_static_inventory() -> None:
    inventory = TopicAdapter.from_options(
        {
            "topic": {
                "name": "topic",
                "type": get_import_name(FakeTopic),
            },
        },
        services=None,
    )
    topic: FakeTopic = t.cast(FakeTopic, inventory.topic)

    messages = await alib.list(inventory.iterate())

    assert topic.processing == 10
    for i, ctx in enumerate(messages):
        async with ctx as message:
            assert message.args["x"] == i
            assert message.cursor is None
            assert topic.processed == i
