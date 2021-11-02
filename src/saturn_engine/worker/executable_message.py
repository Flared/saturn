import contextlib
from typing import AsyncContextManager
from typing import Optional

from saturn_engine.core import PipelineMessage
from saturn_engine.worker.parkers import Parkers

from .resources_manager import ResourceData
from .resources_manager import ResourcesContext


class ExecutableMessage:
    def __init__(
        self,
        *,
        message: PipelineMessage,
        parker: Parkers,
        message_context: Optional[AsyncContextManager] = None
    ):
        self.message = message
        self.context = contextlib.AsyncExitStack()
        if message_context:
            self.context.push_async_exit(message_context)
        self.parker = parker

    def park(self) -> None:
        self.parker.park(id(self))

    async def unpark(self) -> None:
        await self.parker.unpark(id(self))

    async def attach_resources(
        self, resources_context: ResourcesContext
    ) -> dict[str, ResourceData]:
        resources = await self.context.enter_async_context(resources_context)
        resources_data = {k: ({"id": r.id} | r.data) for k, r in resources.items()}
        self.message.update_with_resources(resources_data)
        return resources
