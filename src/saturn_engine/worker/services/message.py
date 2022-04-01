from collections.abc import AsyncGenerator

from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services.hooks import MessagePublished

from . import MinimalService


class MessageTagger(MinimalService):
    name = "message_tagger"

    async def open(self) -> None:
        self.services.hooks.message_published.register(self.on_message_published)

    async def on_message_published(
        self, message: MessagePublished
    ) -> AsyncGenerator[None, None]:
        message.output.message.tags["parent_message_id"] = message.message.message.id
        message.output.message.tags[
            "root_message_id"
        ] = message.message.message.tags.get(
            "root_message_id", message.message.message.id
        )
        yield

    async def on_message_polled(self, message: PipelineMessage) -> None:
        message.message.tags.setdefault("root_message_id", message.message.id)
