from collections.abc import AsyncGenerator
from collections.abc import Generator

import opentelemetry.propagate
from opentelemetry import trace
from opentelemetry.util import types as otel_types

from saturn_engine.core import PipelineResults
from saturn_engine.worker.executors.bootstrap import PipelineBootstrap
from saturn_engine.worker.pipeline_message import PipelineMessage

from .. import MinimalService


class Tracer(MinimalService):
    name = "tracing"

    async def open(self) -> None:
        self.tracer = trace.get_tracer(__name__)
        self.services.hooks.message_executed.register(self.on_message_executed)
        self.services.hooks.executor_initialized.register(on_executor_initialized)

    async def on_message_executed(
        self, message: PipelineMessage
    ) -> AsyncGenerator[None, PipelineResults]:
        operation_name = "worker executing"
        with self.tracer.start_as_current_span(
            operation_name,
            kind=trace.SpanKind.PRODUCER,
            attributes=message_attributes(message),
        ) as span:

            opentelemetry.propagate.inject(
                message.message.metadata.setdefault("tracing", {})
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
            message.message.metadata.get("tracing", {})
        )

        operation_name = "executor executing"
        with self.tracer.start_as_current_span(
            operation_name,
            context=tracectx,
            kind=trace.SpanKind.CONSUMER,
            attributes=message_attributes(message),
        ):
            yield


def message_attributes(message: PipelineMessage) -> otel_types.Attributes:
    return {
        "saturn.message.id": message.message.id,
        "saturn.pipeline.name": message.info.name,
    } | {f"saturn.message.tags.{k}": v for k, v in message.message.tags.items()}
