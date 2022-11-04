import typing as t

from collections.abc import AsyncGenerator
from collections.abc import Generator
from collections.abc import Mapping

import opentelemetry.propagate
from opentelemetry import trace
from opentelemetry.util.types import AttributeValue

from saturn_engine.core import PipelineResults
from saturn_engine.worker.executors.bootstrap import PipelineBootstrap
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.pipeline_message import PipelineMessage

from .. import BaseServices
from .. import Service

_METADATA_KEY: t.Final[str] = "tracing"


class Tracer(Service[BaseServices, "Tracer.Options"]):
    name = "tracer"

    class Options:
        rate: float = 0.0

    async def open(self) -> None:
        self.tracer = trace.get_tracer(__name__)
        self.services.hooks.message_executed.register(self.on_message_executed)
        self.services.hooks.executor_initialized.register(on_executor_initialized)

    async def on_message_executed(
        self, xmsg: ExecutableMessage
    ) -> AsyncGenerator[None, PipelineResults]:
        operation_name = "worker executing"

        sampling_attributes = {"saturn.sampling.rate": self.options_from(xmsg).rate}

        with self.tracer.start_as_current_span(
            operation_name,
            kind=trace.SpanKind.PRODUCER,
            attributes={
                **sampling_attributes,
                **executable_message_attributes(xmsg),
            },
        ) as span:

            opentelemetry.propagate.inject(
                xmsg.message.message.metadata.setdefault(_METADATA_KEY, {})
            )

            results = yield

            span.set_attribute("saturn.outputs.count", len(results.outputs))


def on_executor_initialized(bootstrapper: PipelineBootstrap) -> None:
    pipeline_tracer = PipelineTracer()
    bootstrapper.pipeline_hook.register(pipeline_tracer.on_pipeline_executed)


class PipelineTracer:
    def __init__(self) -> None:
        self.tracer = trace.get_tracer(__name__)

    def on_pipeline_executed(
        self, message: PipelineMessage
    ) -> Generator[None, PipelineResults, None]:
        tracectx = opentelemetry.propagate.extract(
            message.message.metadata.get(_METADATA_KEY, {})
        )

        operation_name = "executor executing"
        with self.tracer.start_as_current_span(
            operation_name,
            context=tracectx,
            kind=trace.SpanKind.CONSUMER,
            attributes=pipeline_message_attributes(message),
        ):
            yield


def executable_message_attributes(
    xmsg: ExecutableMessage,
) -> Mapping[str, AttributeValue]:

    return {
        "saturn.job.name": xmsg.queue.name,
        "saturn.input.name": xmsg.queue.definition.input.name,
    } | pipeline_message_attributes(xmsg.message)


def pipeline_message_attributes(
    message: PipelineMessage,
) -> Mapping[str, AttributeValue]:
    return {
        "saturn.message.id": message.id,
        "saturn.resources.names": [n for n in message.resource_names if n],
        "saturn.pipeline.name": message.info.name,
    } | {f"saturn.message.tags.{k}": v for k, v in message.message.tags.items()}
