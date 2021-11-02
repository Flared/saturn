from abc import abstractmethod
from collections.abc import AsyncGenerator
from typing import AsyncContextManager
from typing import TypeVar
from typing import Union

from saturn_engine.core import PipelineMessage
from saturn_engine.core import QueuePipeline
from saturn_engine.core import TopicMessage
from saturn_engine.utils.options import OptionsSchema

from ..executable_message import ExecutableMessage
from ..parkers import Parkers

T = TypeVar("T")

QueueMessage = Union[AsyncContextManager[TopicMessage], TopicMessage]


class TopicReader(OptionsSchema):
    @abstractmethod
    async def run(self) -> AsyncGenerator[QueueMessage, None]:
        for _ in ():
            yield _


class ExecutableQueue:
    def __init__(self, queue: TopicReader, pipeline: QueuePipeline):
        self.queue = queue
        self.parkers = Parkers()
        self.pipeline = pipeline

    async def run(self) -> AsyncGenerator[ExecutableMessage, None]:
        async for message in self.queue.run():
            await self.parkers.wait()
            context = None
            if isinstance(message, AsyncContextManager):
                context = message
                message = await message.__aenter__()

            yield ExecutableMessage(
                parker=self.parkers,
                message=PipelineMessage(
                    info=self.pipeline.info, message=message.extend(self.pipeline.args)
                ),
                message_context=context,
            )


class Publisher(OptionsSchema):
    def __init__(self, options: object) -> None:
        pass

    async def push(self, message: TopicMessage) -> None:
        pass
