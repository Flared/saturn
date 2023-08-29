import typing as t

import asyncio
import dataclasses
from contextlib import asynccontextmanager
from datetime import datetime
from datetime import timedelta

from saturn_engine.core.topic import TopicMessage
from saturn_engine.utils import utcnow
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import fromdict
from saturn_engine.worker.services import Services
from saturn_engine.worker.topics.rabbitmq import RabbitMQTopic


@dataclasses.dataclass
class DelayedTopicMetadata:
    not_before: str


class DelayedTopic(RabbitMQTopic):
    _METADATA_NAME: t.Final[str] = "delayed_topic"

    @dataclasses.dataclass
    class Options(RabbitMQTopic.Options):
        delay: float = 60

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        super().__init__(options=options, services=services, **kwargs)
        self.delay = options.delay

    async def run(
        self,
    ) -> t.AsyncGenerator[t.AsyncContextManager[TopicMessage], None]:
        async for message_context in super().run():
            yield self.sleep_and_yield(message_context)

    @asynccontextmanager
    async def sleep_and_yield(
        self, message_context: t.AsyncContextManager[TopicMessage]
    ) -> t.AsyncIterator[TopicMessage]:
        async with message_context as message:
            meta = fromdict(
                message.metadata[DelayedTopic._METADATA_NAME], DelayedTopicMetadata
            )

            not_before = datetime.fromisoformat(meta.not_before)
            sleep_time = (not_before - utcnow()).total_seconds()
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

            yield message

    async def publish(
        self,
        message: TopicMessage,
        wait: bool,
    ) -> bool:
        message.metadata[DelayedTopic._METADATA_NAME] = asdict(
            DelayedTopicMetadata(
                not_before=(utcnow() + timedelta(seconds=self.delay)).isoformat(),
            )
        )

        return await super().publish(message, wait)
