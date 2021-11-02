import asyncio
import dataclasses
from collections.abc import AsyncGenerator

from saturn_engine.core import TopicMessage
from saturn_engine.utils.log import getLogger

from . import Topic


class DummyTopic(Topic):
    """A dummy queue that yield a message every second"""

    @dataclasses.dataclass
    class Options:
        name: str
        sleep_time: float = 1

    def __init__(self, options: Options, **kwargs: object) -> None:
        self.options = options
        self.logger = getLogger(__name__, self)

    async def run(self) -> AsyncGenerator[TopicMessage, None]:
        while True:
            self.logger.info("get/before_sleep [q=%s]", self.options.name)
            await asyncio.sleep(self.options.sleep_time)
            self.logger.info("get/after_sleep [q=%s]", self.options.name)
            yield TopicMessage(args={"msg": f"hello - {self.options.name}"})
