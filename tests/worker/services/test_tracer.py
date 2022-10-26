import typing as t

import pytest

from saturn_engine.config import Config
from saturn_engine.core import PipelineResults
from saturn_engine.worker.executors import Executor
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.services.tracing import Tracer
from tests.utils.span_exporter import InMemorySpanExporter


@pytest.fixture
def config(config: Config) -> Config:
    return config.load_object(
        {
            "services_manager": {
                "services": [
                    "saturn_engine.worker.services.tracing.Tracer",
                ]
            }
        }
    )


@pytest.mark.asyncio
async def test_logger_message_executed(
    services_manager: ServicesManager,
    executor: Executor,
    message_maker: t.Callable[..., PipelineMessage],
    span_exporter: InMemorySpanExporter,
) -> None:
    services_manager.services.cast_service(Tracer)
    message = message_maker()

    @services_manager.services.s.hooks.message_executed.emit
    async def scope(message: PipelineMessage) -> PipelineResults:
        return await executor.process_message(message)

    await scope(message)

    traces = span_exporter.get_finished_traces()
    assert len(traces) == 1
    assert traces[0].otel_span.name == "worker executing"
    assert traces[0].otel_span.attributes == {
        "saturn.message.id": message.message.id,
        "saturn.pipeline.name": "tests.conftest.pipeline",
        "saturn.outputs.count": 0,
    }

    assert traces[0].children[0].otel_span.name == "executor executing"
    assert traces[0].children[0].otel_span.attributes == {
        "saturn.message.id": message.message.id,
        "saturn.pipeline.name": "tests.conftest.pipeline",
    }
