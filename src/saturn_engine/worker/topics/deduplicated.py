import dataclasses
from collections.abc import AsyncGenerator
from saturn_engine.core.topic import TopicMessage
from saturn_engine.worker.services import Services
from saturn_engine.core.api import TopicItem

from . import Topic
from . import TopicOutput


class DeduplicatedTopic(Topic):
    """
    A topic that skips messages that were queued before the
    execution of an identical message.

    For example:
    - Message 1: "content1" is queued at 14h00.
    - Message 2: "content1" is queued at 14h05.
    - Message 1 is executed at 14h10.
    - Message 2 is skipped.
    """

    @dataclasses.dataclass
    class Options:
        topic: TopicItem

    def __init__(
        self,
        options: Options,
        services: Services,
        **kwargs: object,
    ) -> None:
        # This import must be done late since work_factory depends on this module.
        from saturn_engine.worker.work_factory import build_topic
        self.topic: Topic = build_topic(options.topic, services=services)

    async def run(self) -> AsyncGenerator[TopicOutput, None]:
        generator = self.topic.run()
        try:
            async for message in generator:
                yield message
        finally:
            await generator.aclose()
            await self.close()

    async def close(self) -> None:
        return await self.topic.close()
