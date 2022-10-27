from typing import AsyncContextManager
from typing import Optional

import contextlib
from collections.abc import AsyncGenerator

from saturn_engine.core import ResourceUsed
from saturn_engine.core.api import QueueItem
from saturn_engine.worker.executors.parkers import Parkers
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.resources.manager import ResourceContext
from saturn_engine.worker.resources.manager import ResourcesContext
from saturn_engine.worker.services import Services
from saturn_engine.worker.topic import Topic


class ExecutableMessage:
    def __init__(
        self,
        *,
        queue: "ExecutableQueue",
        message: PipelineMessage,
        parker: Parkers,
        output: dict[str, list[Topic]],
        message_context: Optional[AsyncContextManager] = None,
    ):
        self.message = message
        self._context = contextlib.AsyncExitStack()
        if message_context:
            self._context.push_async_exit(message_context)
        self._parker = parker
        self.output = output
        self.resources: dict[str, ResourceContext] = {}
        self.queue = queue

    @property
    def id(self) -> str:
        return self.message.id

    def park(self) -> None:
        self._parker.park(id(self))

    async def unpark(self) -> None:
        await self._parker.unpark(id(self))

    async def attach_resources(
        self, resources_context: ResourcesContext
    ) -> dict[str, dict[str, object]]:
        self.resources = await self._context.enter_async_context(resources_context)
        resources_data = {
            k: ({"name": r.resource.name} | r.resource.data)
            for k, r in self.resources.items()
            if r.resource
        }
        self.message.update_with_resources(resources_data)
        return resources_data

    def update_resources_used(self, resources_used: list[ResourceUsed]) -> None:
        if not self.resources:
            return

        for resource_used in resources_used:
            self.resources[resource_used.type].release_later(resource_used.release_at)

    def __str__(self) -> str:
        return str(self.message.message.id)


class ExecutableQueue:
    def __init__(
        self,
        *,
        definition: QueueItem,
        topic: Topic,
        output: dict[str, list[Topic]],
        services: Services,
    ):
        self.definition = definition
        self.name = definition.name
        self.pipeline = definition.pipeline
        self.executor = definition.executor

        self.topic = topic
        self.output = output
        self.services = services

        self.parkers = Parkers()
        self.iterable = self.run()

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
                executable_message = ExecutableMessage(
                    queue=self,
                    parker=self.parkers,
                    message=pipeline_message,
                    message_context=context,
                    output=self.output,
                )
                await self.services.s.hooks.message_polled.emit(executable_message)
                await self.parkers.wait()
                yield executable_message
        finally:
            await self.close()

    async def close(self) -> None:
        # TODO: don't clean topics here once topics are shared between jobs.
        await self.topic.close()
        for topics in self.output.values():
            for topic in topics:
                await topic.close()
