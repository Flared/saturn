import typing as t

import asyncio
from unittest.mock import Mock

import pytest

from saturn_engine.core import PipelineResults
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.services.hooks import MessagePublished
from saturn_engine.worker.services.hooks import ResultsProcessed
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
    for _ in range(8):
        xmsg: ExecutableMessage = Mock()
        xmsg.message.info.name = pipeline_name
        xmsg.queue.definition.executor = "default"
        xmsgs.append(xmsg)

    any_: t.Any = None
    pipeline_params = {"pipeline": pipeline_name, "executor": "default"}
    results = PipelineResults(outputs=[], resources=[])
    metric = services_manager.services.cast_service(UsageMetrics)

    # Setup things such that:
    # 1 message is polling
    # 1 message is scheduling
    # 1 message is submitting
    # 2 message are executing
    # 3 message are processing results
    # * 1 is publishing
    # * 1 is waiting on publish
    for xmsg in xmsgs:
        await metric.on_message_polled(xmsg)
    for xmsg in xmsgs[1:]:
        await metric.on_message_scheduled(xmsg)
    for xmsg in xmsgs[2:]:
        await metric.on_message_submitted(xmsg)

    executing_generators = []
    for xmsg in xmsgs[3:5]:
        hook_generator = metric.on_message_executed(xmsg)
        await hook_generator.__anext__()
        executing_generators.append(hook_generator)

    processing_generators = []
    for xmsg in xmsgs[5:]:
        hook_generator = metric.on_message_executed(xmsg)
        await hook_generator.__anext__()
        with pytest.raises(StopAsyncIteration):
            await hook_generator.asend(results)

        proc_hook_generator = metric.on_results_processed(
            ResultsProcessed(xmsg, results)
        )
        await proc_hook_generator.__anext__()
        processing_generators.append(proc_hook_generator)

    publishing_generators = []
    for xmsg in xmsgs[6:]:
        pub_hook_generator = metric.on_message_published(
            MessagePublished(xmsg, any_, any_)
        )
        await pub_hook_generator.__anext__()
        publishing_generators.append(pub_hook_generator)

    waiting_publish_generator = metric.on_output_blocked(
        MessagePublished(xmsgs[7], any_, any_)
    )
    await waiting_publish_generator.__anext__()

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
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"state": "processing_results"},
            ),
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"state": "publishing"},
            ),
            metrics_capture.create_number_data_point(
                1,
                attributes=pipeline_params | {"state": "waiting_publish"},
            ),
        ],
    )

    with pytest.raises(StopAsyncIteration):
        await waiting_publish_generator.__anext__()

    for pub_hook_generator in publishing_generators:
        with pytest.raises(StopAsyncIteration):
            await pub_hook_generator.asend(None)

    await metric.on_message_scheduled(xmsgs[0])
    await asyncio.sleep(1)
    await metric.on_message_submitted(xmsgs[0])

    for hook_generator in executing_generators:
        with pytest.raises(StopAsyncIteration):
            await hook_generator.asend(results)

    for proc_hook_generator in processing_generators:
        with pytest.raises(StopAsyncIteration):
            await proc_hook_generator.asend(None)

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
            metrics_capture.create_number_data_point(
                0.75,
                attributes=pipeline_params | {"state": "processing_results"},
            ),
            metrics_capture.create_number_data_point(
                0,
                attributes=pipeline_params | {"state": "publishing"},
            ),
            metrics_capture.create_number_data_point(
                0,
                attributes=pipeline_params | {"state": "waiting_publish"},
            ),
        ],
    )
