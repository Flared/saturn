from typing import AsyncContextManager

from collections.abc import AsyncGenerator

from saturn_engine.core import QueuePipeline
from saturn_engine.worker.services import Services

from .executable_message import ExecutableMessage
from .parkers import Parkers
from .pipeline_message import PipelineMessage
from .topics import Topic


class ExecutableQueue:
    def __init__(
        self,
        *,
        topic: Topic,
        pipeline: QueuePipeline,
        output: dict[str, list[Topic]],
        services: Services
    ):
        self.topic = topic
        self.parkers = Parkers()
        self.pipeline = pipeline
        self.output = output
        self.services = services

    async def run(self) -> AsyncGenerator[ExecutableMessage, None]:
        try:
            async for message in self.topic.run():
                context = None
                if isinstance(message, AsyncContextManager):
                    context = message
                    message = await message.__aenter__()

                pipeline_message = PipelineMessage(
                    info=self.pipeline.info,
                    message=message.extend(self.pipeline.args),
                )
                await self.services.s.hooks.message_polled.emit(pipeline_message)

                await self.parkers.wait()
                yield ExecutableMessage(
                    parker=self.parkers,
                    message=pipeline_message,
                    message_context=context,
                    output=self.output,
                )
        finally:
            await self.close()

    async def close(self) -> None:
        # TODO: don't clean topics here once topics are shared between jobs.
        await self.topic.close()
        for topics in self.output.values():
            for topic in topics:
                await topic.close()
