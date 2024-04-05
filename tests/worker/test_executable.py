import typing as t

import asyncio
import dataclasses
from contextlib import AsyncExitStack
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import asyncstdlib as alib
import pytest

from saturn_engine.config import Config
from saturn_engine.core import TopicMessage
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import QueueItemWithState
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.executors.executable import ExecutableQueue
from saturn_engine.worker.topic import Topic
from saturn_engine.worker.topic import TopicOutput
from saturn_engine.worker.topics import MemoryTopic
from saturn_engine.worker.topics.static import StaticTopic
from tests.utils import TimeForwardLoop


class ServiceA:
    x: str = "foo"


class ServiceB:
    y: str
    z: str = "biz"


@pytest.fixture
def config(config: Config) -> Config:
    return (
        config.load_object(
            {
                "test-service-b": {"y": "bar"},
            }
        )
        .register_interface("test-service-a", ServiceA)
        .register_interface("test-service-b", ServiceB)
    )


def test_config_override(
    executable_queue_maker: t.Callable[..., ExecutableQueue],
    executable_maker: t.Callable[..., ExecutableMessage],
    fake_queue_item: QueueItem,
    config: Config,
) -> None:
    # No configuration override either in queue or message.
    xqueue_no_conf = executable_queue_maker()

    assert xqueue_no_conf.config.cast_namespace("test-service-a", ServiceA).x == "foo"
    assert xqueue_no_conf.config.cast_namespace("test-service-b", ServiceB).y == "bar"
    assert xqueue_no_conf.config.cast_namespace("test-service-b", ServiceB).z == "biz"

    xmsg = executable_maker(executable_queue=xqueue_no_conf)

    assert xmsg.config.cast_namespace("test-service-a", ServiceA).x == "foo"
    assert xmsg.config.cast_namespace("test-service-b", ServiceB).y == "bar"
    assert xmsg.config.cast_namespace("test-service-b", ServiceB).z == "biz"

    # Configuration override from queue.
    queue_item = dataclasses.replace(
        fake_queue_item, config={"test-service-b": {"y": "bar-queue"}}
    )
    xqueue_conf_y = executable_queue_maker(definition=queue_item)
    xmsg = executable_maker(executable_queue=xqueue_conf_y)

    assert xmsg.config.cast_namespace("test-service-a", ServiceA).x == "foo"
    assert xmsg.config.cast_namespace("test-service-b", ServiceB).y == "bar-queue"
    assert xmsg.config.cast_namespace("test-service-b", ServiceB).z == "biz"

    # Configuration override from message.
    message = TopicMessage(args={}, config={"test-service-b": {"y": "bar-message"}})
    xmsg = executable_maker(executable_queue=xqueue_no_conf, message=message)

    assert xmsg.config.cast_namespace("test-service-a", ServiceA).x == "foo"
    assert xmsg.config.cast_namespace("test-service-b", ServiceB).y == "bar-message"
    assert xmsg.config.cast_namespace("test-service-b", ServiceB).z == "biz"

    # Configuration message override queue
    message = TopicMessage(args={}, config={"test-service-b": {"y": "bar-message"}})
    xmsg = executable_maker(executable_queue=xqueue_conf_y, message=message)

    assert xmsg.config.cast_namespace("test-service-a", ServiceA).x == "foo"
    assert xmsg.config.cast_namespace("test-service-b", ServiceB).y == "bar-message"
    assert xmsg.config.cast_namespace("test-service-b", ServiceB).z == "biz"

    # Configuration override from queue and message
    message = TopicMessage(args={}, config={"test-service-b": {"z": "biz-message"}})
    xmsg = executable_maker(executable_queue=xqueue_conf_y, message=message)

    assert xmsg.config.cast_namespace("test-service-a", ServiceA).x == "foo"
    assert xmsg.config.cast_namespace("test-service-b", ServiceB).y == "bar-queue"
    assert xmsg.config.cast_namespace("test-service-b", ServiceB).z == "biz-message"


async def test_executable_close(
    executable_queue_maker: t.Callable[..., ExecutableQueue],
    running_event_loop: TimeForwardLoop,
) -> None:
    output_topic = MemoryTopic(MemoryTopic.Options(name="output_topic"))
    output_topic.close = AsyncMock()  # type: ignore[assignment]

    input_topic = MemoryTopic(MemoryTopic.Options(name="input_topic"))
    input_topic.close = AsyncMock()  # type: ignore[assignment]
    await input_topic.publish(TopicMessage(args={}), wait=True)

    executable_queue = executable_queue_maker(
        topic=input_topic, output={"default": [output_topic]}
    )

    xmsg_iter = executable_queue.iterable

    xmsg_ctx = await alib.anext(xmsg_iter)
    async with xmsg_ctx._context:
        async with running_event_loop.until_idle():
            close_task = asyncio.create_task(executable_queue.close())
        input_topic.close.assert_awaited()
        output_topic.close.assert_not_called()
        assert not close_task.done()

    await close_task
    output_topic.close.assert_awaited()


async def test_executable_context_error(
    executable_queue_maker: t.Callable[..., ExecutableQueue],
) -> None:
    class FakeTopic(Topic):
        async def run(self) -> t.AsyncGenerator[TopicOutput, None]:
            yield self.bad_context_1()
            yield TopicMessage(args={"x": 1})

        @asynccontextmanager
        async def bad_context_1(self) -> t.AsyncIterator[TopicMessage]:
            raise ValueError("Oh no")
            yield TopicMessage()

    executable_queue = executable_queue_maker(topic=FakeTopic.from_options({}))
    messages = []
    async for xmsg in executable_queue.run():
        async with xmsg._context:
            messages.append(xmsg.message.message)
    assert [m.args for m in messages] == [{"x": 1}]


async def test_execurablt_concurrency(
    executable_queue_maker: t.Callable[..., ExecutableQueue],
    fake_queue_item: QueueItemWithState,
    running_event_loop: TimeForwardLoop,
) -> None:
    topic = StaticTopic(
        options=StaticTopic.Options(
            messages=[{"args": {}}],
            cycle=True,
        )
    )
    fake_queue_item.config["job"] = {"max_concurrency": 2}
    xqueue = executable_queue_maker(topic=topic, definition=fake_queue_item)

    async with alib.scoped_iter(xqueue.run()) as xrun, AsyncExitStack() as stack:
        msg1 = await xrun.__anext__()
        msg2 = await xrun.__anext__()
        async with running_event_loop.until_idle():
            next_task = asyncio.create_task(alib.anext(xrun))
        assert not next_task.done()

        async with running_event_loop.until_idle():
            await stack.enter_async_context(msg1._context)
        assert not next_task.done()

        async with running_event_loop.until_idle():
            async with stack:
                pass
        assert next_task.done()

        await stack.enter_async_context(msg2._context)
        await stack.enter_async_context(next_task.result()._context)
