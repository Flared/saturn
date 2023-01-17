import typing as t

import logging

import pytest

from saturn_engine.core import TopicMessage
from saturn_engine.core.error import ErrorMessageArgs
from saturn_engine.utils.traceback_data import TracebackData
from saturn_engine.worker.executors.bootstrap import RemoteException
from saturn_engine.worker.services.loggers.logger import topic_message_data
from saturn_engine.worker.topics import LoggingTopic


def generate_exception(msg: str) -> BaseException:
    return Exception(msg)


@pytest.mark.asyncio
async def test_logging_topic(caplog: t.Any) -> None:
    messages = [
        TopicMessage(id="0", args={"n": 1}),
        TopicMessage(id="1", args={"n": 2}),
    ]
    error_messages = [
        TopicMessage(
            id="2",
            args={
                "error": ErrorMessageArgs(
                    type="Exception",
                    module="test_logging_topic.py",
                    message="TEST_EXCEPTION_2",
                    traceback=TracebackData.from_exception(
                        generate_exception("TEST_EXCEPTION_2")
                    ),
                )
            },
        ),
        TopicMessage(
            id="3",
            args={
                "error": ErrorMessageArgs(
                    type="Exception",
                    module="test_logging_topic.py",
                    message="TEST_EXCEPTION_3",
                    traceback=TracebackData.from_exception(
                        generate_exception("TEST_EXCEPTION_3")
                    ),
                )
            },
        ),
    ]

    # Test normal logging
    with caplog.at_level(logging.DEBUG):
        topic = LoggingTopic.from_options(
            {"name": "info-logging-topic", "level": "INFO"}
        )
        # Test 1
        await topic.publish(messages[0], wait=True)
        r = caplog.records[-1]
        assert r.message == "Message published: 0"
        assert r.data == topic_message_data(messages[0], verbose=True)
        # Test 2
        await topic.publish(messages[1], wait=True)
        r = caplog.records[-1]
        assert r.message == "Message published: 1"
        assert r.data == topic_message_data(messages[1], verbose=True)
        await topic.close()

    # Test error logging
    with caplog.at_level(logging.ERROR):
        topic = LoggingTopic.from_options(
            {"name": "error-logging-topic", "level": "ERROR"}
        )
        # Test 1
        await topic.publish(error_messages[0], wait=True)
        r = caplog.records[-1]
        assert r.message == "TEST_EXCEPTION_2"
        mdata = topic_message_data(error_messages[0], verbose=True)
        del mdata["args"]
        assert r.data == mdata | {
            "exception": str(
                RemoteException(
                    t.cast(ErrorMessageArgs, error_messages[0].args["error"]).traceback
                )
            ),
        }
        # Test 2
        await topic.publish(error_messages[1], wait=True)
        r = caplog.records[-1]
        assert r.message == "TEST_EXCEPTION_3"
        mdata = topic_message_data(error_messages[1], verbose=True)
        del mdata["args"]
        assert r.data == mdata | {
            "exception": str(
                RemoteException(
                    t.cast(ErrorMessageArgs, error_messages[1].args["error"]).traceback
                )
            ),
        }
        await topic.close()
