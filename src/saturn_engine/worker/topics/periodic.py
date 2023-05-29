import asyncio
import dataclasses
import time
from collections.abc import AsyncGenerator

from croniter import croniter

from saturn_engine.core import MessageId
from saturn_engine.core import TopicMessage

from . import Topic


class PeriodicTopic(Topic):
    @dataclasses.dataclass
    class Options:
        interval: str

    def __init__(self, options: Options, **kwargs: object) -> None:
        self.options = options

    async def run(self) -> AsyncGenerator[TopicMessage, None]:
        start_time = time.time()
        interval_iterator = croniter(self.options.interval, start_time)

        while True:
            next_tick = interval_iterator.get_next()
            wait_time = next_tick - time.time()
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            yield TopicMessage(id=MessageId(str(next_tick)), args={})

    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        raise ValueError("Cannot publish on periodic topic")
