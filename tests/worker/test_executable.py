import typing as t

import dataclasses

import pytest

from saturn_engine.config import Config
from saturn_engine.core import TopicMessage
from saturn_engine.core.api import QueueItem
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.executors.executable import ExecutableQueue


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
