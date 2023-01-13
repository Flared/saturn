from saturn_engine.worker.executors.executable import ExecutableMessage

from . import MinimalService


class LabelsPropagator(MinimalService):
    name = "labels_propagator"

    async def open(self) -> None:
        self.services.hooks.message_polled.register(self.on_message_polled)

    async def on_message_polled(self, xmsg: ExecutableMessage) -> None:
        metadata_labels = xmsg.message.message.metadata.setdefault("labels", {})
        for k, v in xmsg.queue.definition.labels.items():
            metadata_labels.setdefault(k, v)
