import typing as t

import pytest

from saturn_engine.config import Config
from saturn_engine.core import PipelineResults
from saturn_engine.core import TopicMessage
from saturn_engine.worker.executors import Executor
from saturn_engine.worker.executors.executable import ExecutableMessage
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
async def test_trace_message_executed(
    services_manager: ServicesManager,
    executor: Executor,
    executable_maker: t.Callable[..., ExecutableMessage],
    span_exporter: InMemorySpanExporter,
) -> None:
    services_manager.services.cast_service(Tracer)
    xmsg = executable_maker(
        message=TopicMessage(args={}, config={"tracer": {"rate": 0.5}})
    )

    @services_manager.services.s.hooks.message_executed.emit
    async def scope(xmsg: ExecutableMessage) -> PipelineResults:
        return await executor.process_message(xmsg)

    await services_manager.services.s.hooks.message_polled.emit(xmsg)
    await scope(xmsg)

    traces = span_exporter.get_finished_traces()
    assert len(traces) == 1
    assert traces[0].otel_span.name == "worker executing"
    assert traces[0].otel_span.attributes == {
        "saturn.job.name": "fake-queue",
        "saturn.job.labels.owner": "team-saturn",
        "saturn.input.name": "fake-topic",
        "saturn.resources.names": (),
        "saturn.message.id": xmsg.id,
        "saturn.pipeline.name": "tests.conftest.pipeline",
        "saturn.outputs.count": 0,
        "saturn.sampling.rate": 0.5,
    }

    assert traces[0].children[0].otel_span.name == "executor executing"
    assert traces[0].children[0].otel_span.attributes == {
        "saturn.resources.names": (),
        "saturn.message.id": xmsg.id,
        "saturn.pipeline.name": "tests.conftest.pipeline",
    }
