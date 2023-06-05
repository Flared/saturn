import asyncio
import dataclasses
from unittest.mock import Mock

import pytest

from saturn_engine.config import Config
from saturn_engine.core import PipelineResults
from saturn_engine.core import api
from saturn_engine.core.api import ComponentDefinition
from saturn_engine.core.api import LockResponse
from saturn_engine.core.api import PipelineInfo
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import QueuePipeline
from saturn_engine.core.api import ResourceItem
from saturn_engine.core.api import ResourcesProviderItem
from saturn_engine.utils.inspect import get_import_name
from saturn_engine.worker.broker import Broker
from saturn_engine.worker.executors import ExecutableMessage
from saturn_engine.worker.executors import Executor
from saturn_engine.worker.resources.provider import ProvidedResource
from saturn_engine.worker.resources.provider import ResourcesProvider
from tests.utils import register_hooks_handler
from tests.utils.metrics import MetricsCapture
from tests.utils.span_exporter import InMemorySpanExporter
from tests.worker.conftest import FakeResource


class FakeExecutor(Executor):
    concurrency = 5
    done_event: asyncio.Event

    @dataclasses.dataclass
    class Options:
        ok: bool

    def __init__(self, options: Options, services: object) -> None:
        assert options.ok is True

    async def process_message(self, message: ExecutableMessage) -> PipelineResults:
        pipeline_message = message.message
        assert isinstance(pipeline_message.message.args["resource"], dict)
        assert pipeline_message.message.args["resource"]["data"] == "fake"
        if pipeline_message.message.args["n"] == 999:
            FakeExecutor.done_event.set()
        return PipelineResults(outputs=[], resources=[])


class FakeResourcesProvider(ResourcesProvider["FakeResourcesProvider"]):
    @dataclasses.dataclass
    class Options:
        pass

    async def open(self) -> None:
        await self.add(ProvidedResource(name="fake-resource", data={"foo": "bar"}))


def pipeline(resource: FakeResource) -> None:
    ...


@pytest.fixture
def config(config: Config) -> Config:
    return config.load_object(
        {
            "services_manager": {
                "services": [
                    "saturn_engine.worker.services.labels_propagator.LabelsPropagator",
                    "saturn_engine.worker.services.tracing.Tracer",
                    "saturn_engine.worker.services.metrics.Metrics",
                ]
            }
        }
    )


@pytest.mark.asyncio
async def test_broker_dummy(
    broker: Broker,
    config: Config,
    worker_manager_client: Mock,
    span_exporter: InMemorySpanExporter,
    metrics_capture: MetricsCapture,
) -> None:
    FakeExecutor.done_event = asyncio.Event()

    hooks_handler = register_hooks_handler(broker.services_manager.services)
    pipeline_info = PipelineInfo.from_pipeline(pipeline)
    worker_manager_client.lock.return_value = LockResponse(
        items=[
            QueueItem(
                name="j1",
                input=ComponentDefinition(
                    name="dummy", type="DummyInventory", options={"count": 10000}
                ),
                pipeline=QueuePipeline(
                    args={},
                    info=pipeline_info,
                ),
                labels={"owner": "team-saturn"},
                output={},
                executor="e1",
            )
        ],
        resources=[
            ResourceItem(
                name="r1",
                type=FakeResource._typename(),
                data={"data": "fake"},
            ),
        ],
        resources_providers=[
            ResourcesProviderItem(
                name="fake-resources-provider",
                type=get_import_name(FakeResourcesProvider),
                resource_type="FakeProvidedResource",
                options={},
            )
        ],
        executors=[
            api.ComponentDefinition(
                name="e1", type=get_import_name(FakeExecutor), options={"ok": True}
            ),
        ],
    )

    wait_task = asyncio.create_task(FakeExecutor.done_event.wait())
    broker_task = asyncio.create_task(broker.run())
    tasks: set[asyncio.Task] = {wait_task, broker_task}

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    assert wait_task in done
    assert broker_task in pending

    assert hooks_handler.message_polled.await_count == 1001
    assert hooks_handler.message_scheduled.await_count == 1001
    assert hooks_handler.message_submitted.await_count == 1001
    assert hooks_handler.message_executed.before.await_count == 1000
    assert hooks_handler.message_executed.success.await_count == 1000
    assert hooks_handler.message_executed.errors.await_count == 0
    assert hooks_handler.message_published.before.await_count == 0
    assert hooks_handler.message_published.success.await_count == 0
    assert hooks_handler.message_published.errors.await_count == 0

    # The provider was open.
    assert (
        len(broker.resources_manager.resources["FakeProvidedResource"].availables) == 1
    )

    # Test tracing
    exported_traces = span_exporter.get_finished_traces()
    assert len(exported_traces) == 1000
    assert exported_traces[0].otel_span.name == "worker executing"
    assert exported_traces[0].otel_span.attributes
    assert exported_traces[0].otel_span.attributes["saturn.message.id"] == "0"
    assert (
        exported_traces[0].otel_span.attributes["saturn.labels.owner"] == "team-saturn"
    )

    # Test metrics
    pipeline_params = {"pipeline": pipeline_info.name}
    metrics_capture.assert_metric_expected(
        "saturn.pipeline.message",
        [
            metrics_capture.create_number_data_point(
                1001,
                attributes=pipeline_params | {"state": "polled"},
            ),
            metrics_capture.create_number_data_point(
                1001,
                attributes=pipeline_params | {"state": "scheduled"},
            ),
            metrics_capture.create_number_data_point(
                1001,
                attributes=pipeline_params | {"state": "submitted"},
            ),
            metrics_capture.create_number_data_point(
                1000,
                attributes=pipeline_params | {"state": "executing"},
            ),
            metrics_capture.create_number_data_point(
                1000,
                attributes=pipeline_params | {"state": "success"},
            ),
        ],
    )

    broker_task.cancel()
    await broker_task
