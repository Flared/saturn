import typing as t

import dataclasses
from contextlib import AsyncExitStack

from saturn_engine.core.api import ComponentDefinition
from saturn_engine.core.types import Cursor
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.inventory import IteratorInventory
from saturn_engine.worker.services import Services


@dataclasses.dataclass
class ItemAdapter(Item):
    _context: AsyncExitStack = None  # type: ignore[assignment]

    async def __aexit__(self, *exc: t.Any) -> t.Optional[bool]:
        return await self._context.__aexit__(*exc)


class TopicAdapter(IteratorInventory):
    @dataclasses.dataclass
    class Options:
        topic: ComponentDefinition

    def __init__(self, options: Options, services: Services, **kwargs: object) -> None:
        # This import must be done late since work_factory depends on this module.
        from saturn_engine.worker.work_factory import build_topic

        self.topic = build_topic(options.topic, services=services)

    async def iterate(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        async for message_ctx in self.topic.run():
            try:
                async with AsyncExitStack() as stack:
                    message = await stack.enter_async_context(message_ctx)
                    yield ItemAdapter(
                        id=message.id,
                        cursor=None,
                        args=message.args,
                        tags=message.tags,
                        metadata=message.metadata,
                        _context=stack.pop_all(),
                    )
            except Exception:
                self.logger.exception("Failed to convert message")
