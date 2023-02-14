from typing import AsyncGenerator

import dataclasses
import logging
from enum import Enum

from saturn_engine.core import TopicMessage
from saturn_engine.core.error import ErrorMessageArgs
from saturn_engine.worker.executors.bootstrap import RemoteException
from saturn_engine.worker.services.loggers.logger import topic_message_data
from saturn_engine.worker.topic import Topic


class Level(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class LoggingTopic(Topic):
    """A topic contains default loggers for error handling
    such as ignoring the error and logging it in debug.
    """

    @dataclasses.dataclass
    class Options:
        name: str
        level: Level

    def __init__(self, options: Options, **kwargs: object) -> None:
        self.level = logging.getLevelName(options.level.value)
        self.name = options.name
        self.logger = logging.getLogger(f"saturn.topics.{self.name}")

    async def run(self) -> AsyncGenerator[TopicMessage, None]:
        raise NotImplementedError()
        yield

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        extra = {"data": topic_message_data(message, verbose=True)}
        msg = f"Message published: {message.id}"

        error_msg = message.args.get("error")
        if isinstance(error_msg, ErrorMessageArgs):
            # remove args from extra since it would be redundant with the traceback
            extra["data"].pop("args")
            # get our error message and put the traceback in our extra data
            msg = error_msg.message
            extra["data"]["exception"] = str(RemoteException(error_msg.traceback))

        self.logger.log(
            self.level,
            msg,
            extra=extra,
        )
        return True
