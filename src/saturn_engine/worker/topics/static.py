from typing import Any

import dataclasses
from collections.abc import AsyncGenerator

from saturn_engine.core import TopicMessage
from saturn_engine.utils.log import getLogger

from . import Topic


class StaticTopic(Topic):
    @dataclasses.dataclass
    class Options:
        messages: list[dict[str, Any]]

    def __init__(self, options: Options, **kwargs: object) -> None:
        self.options = options
        self.logger = getLogger(__name__, self)

    async def run(self) -> AsyncGenerator[TopicMessage, None]:
        for message in self.options.messages:
            yield TopicMessage(id=message["id"], args=message["args"])

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        self.logger.info("publish: %s", message)
        return True
