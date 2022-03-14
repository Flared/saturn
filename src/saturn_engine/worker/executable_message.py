from typing import AsyncContextManager
from typing import Optional

import contextlib

from saturn_engine.core import ResourceUsed
from saturn_engine.worker.parkers import Parkers
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.topic import Topic

from .resources_manager import ResourceContext
from .resources_manager import ResourcesContext


class ExecutableMessage:
    def __init__(
        self,
        *,
        message: PipelineMessage,
        parker: Parkers,
        output: dict[str, list[Topic]],
        message_context: Optional[AsyncContextManager] = None
    ):
        self.message = message
        self.context = contextlib.AsyncExitStack()
        if message_context:
            self.context.push_async_exit(message_context)
        self.parker = parker
        self.output = output
        self.resources: dict[str, ResourceContext] = {}

    def park(self) -> None:
        self.parker.park(id(self))

    async def unpark(self) -> None:
        await self.parker.unpark(id(self))

    async def attach_resources(
        self, resources_context: ResourcesContext
    ) -> dict[str, dict[str, object]]:
        self.resources = await self.context.enter_async_context(resources_context)
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
        return self.message.message.id
