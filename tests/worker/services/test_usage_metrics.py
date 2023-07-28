import asyncio
from unittest.mock import Mock

import pytest

from saturn_engine.core import PipelineResults
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.services.usage_metrics import UsageMetrics
from tests.utils.metrics import MetricsCapture


@pytest.mark.asyncio
async def test_message_metrics(
    services_manager: ServicesManager,
    metrics_capture: MetricsCapture,
    frozen_time: object,
) -> None:
    pipeline_name = "test.fake.pipeline"
    xmsgs = []
    for _ in range(5):
        xmsg: ExecutableMessage = Mock()
        xmsg.message.info.name = pipeline_name
        xmsg.queue.definition.executor = "default"
        xmsgs.append(xmsg)

    pipeline_params = {"pipeline": pipeline_name, "executor": "default"}
    results = PipelineResults(outputs=[], resources=[])
    metric = services_manager.services.cast_service(UsageMetrics)

    for xmsg in xmsgs:
        await metric.on_message_polled(xmsg)
    for xmsg in xmsgs[1:]:
        await metric.on_message_scheduled(xmsg)
    for xmsg in xmsgs[2:]:
        await metric.on_message_submitted(xmsg)

    executing_generators = []
    for xmsg in xmsgs[3:]:
        hook_generator = metric.on_message_executed(xmsg)
        await hook_generator.__anext__()
        executing_generators.append(hook_generator)

    await asyncio.sleep(1)

    metrics_capture.assert_metric_expected(
        "saturn.pipeline.usage",
        [
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"state": "polling"},
            ),
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"state": "scheduling"},
            ),
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"state": "submitting"},
            ),
            metrics_capture.create_number_data_point(
                2,
                attributes=pipeline_params | {"state": "executing"},
            ),
        ],
    )

    await metric.on_message_scheduled(xmsgs[0])
    await asyncio.sleep(1)
    await metric.on_message_submitted(xmsgs[0])

    for hook_generator in executing_generators:
        with pytest.raises(StopAsyncIteration):
            await hook_generator.asend(results)

    await asyncio.sleep(1)
    await metric.on_message_submitted(xmsgs[1])

    await asyncio.sleep(2)

    metrics_capture.collect()
    metrics_capture.assert_metric_expected(
        "saturn.pipeline.usage",
        [
            metrics_capture.create_number_data_point(
                0,
                attributes=pipeline_params | {"state": "polling"},
            ),
            metrics_capture.create_number_data_point(
                0.75,
                attributes=pipeline_params | {"state": "scheduling"},
            ),
            metrics_capture.create_number_data_point(
                2.25,
                attributes=pipeline_params | {"state": "submitting"},
            ),
            metrics_capture.create_number_data_point(
                0.5,
                attributes=pipeline_params | {"state": "executing"},
            ),
        ],
    )
