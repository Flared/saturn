from typing import Any

import dataclasses
import itertools
from collections.abc import AsyncGenerator

from saturn_engine.core import TopicMessage
from saturn_engine.utils.log import getLogger

from . import Topic


class StaticTopic(Topic):
    @dataclasses.dataclass
    class Options:
        messages: list[dict[str, Any]]
        cycle: bool = False

    def __init__(self, options: Options, **kwargs: object) -> None:
        self.options = options
        self.logger = getLogger(__name__, self)

    async def run(self) -> AsyncGenerator[TopicMessage, None]:
        messages_iter = iter(self.options.messages)
        if self.options.cycle:
            messages_iter = itertools.cycle(messages_iter)
        for message in messages_iter:
            yield TopicMessage(**message)

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        self.logger.info("publish: %s", message)
        return True
