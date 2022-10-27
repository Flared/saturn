import typing as t

import asyncio
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import call

import pytest

from saturn_engine.core import PipelineOutput
from saturn_engine.core import PipelineResults
from saturn_engine.core import ResourceUsed
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.services.metrics import BaseMetricsService


class MockMetricsService(BaseMetricsService):
    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        super().__init__(*args, **kwargs)
        self.mock = AsyncMock()

    async def incr(
        self, key: str, *, count: int = 1, params: t.Optional[dict[str, str]] = None
    ) -> None:
        await self.mock.incr(key, count=count, params=params)

    async def timing(
        self, key: str, seconds: float, *, params: t.Optional[dict[str, str]] = None
    ) -> None:
        await self.mock.timing(key, seconds, params=params)


@pytest.mark.asyncio
async def test_metrics(services_manager: ServicesManager) -> None:
    data = Mock()
    pipeline_params = {"pipeline": data.message.info.name}
    metric = MockMetricsService(services_manager.services)
    await metric.on_message_polled(data)
    metric.mock.incr.assert_awaited_once_with(
        "message.polled", count=1, params=pipeline_params
    )
    metric.mock.incr.reset_mock()

    await metric.on_message_scheduled(data)
    metric.mock.incr.assert_awaited_once_with(
        "message.scheduled", count=1, params=pipeline_params
    )
    metric.mock.incr.reset_mock()

    await metric.on_message_submitted(data)
    metric.mock.incr.assert_awaited_once_with(
        "message.submitted", count=1, params=pipeline_params
    )
    metric.mock.incr.reset_mock()

    await metric.on_output_blocked(data)
    metric.mock.incr.assert_awaited_once_with(
        "topic.blocked", count=1, params={"topic": data.name}
    )
    metric.mock.incr.reset_mock()


@pytest.mark.asyncio
async def test_metrics_message_executed(services_manager: ServicesManager) -> None:
    data = Mock()
    pipeline_params = {"pipeline": data.message.info.name}
    metric = MockMetricsService(services_manager.services)

    results = PipelineResults(
        outputs=[PipelineOutput(channel="default", message=data)],
        resources=[ResourceUsed(type="Resource", release_at=10)],
    )
    hook_generator = metric.on_message_executed(data)
    await hook_generator.__anext__()
    with pytest.raises(StopAsyncIteration):
        await asyncio.sleep(1)
        await hook_generator.asend(results)
    assert metric.mock.timing.await_args.args[0] == "message.executed"
    assert metric.mock.timing.await_args.args[1] >= 1
    assert metric.mock.timing.await_args.args[1] < 2
    assert metric.mock.timing.await_args.kwargs["params"] == pipeline_params
    assert metric.mock.incr.await_args_list == [
        call("message.executed.before", count=1, params=pipeline_params),
        call("message.executed.success", count=1, params=pipeline_params),
        call("resource.used", count=1, params={"type": "Resource"}),
        call(
            "message.executed.outputs",
            params=pipeline_params | {"channel": "default"},
            count=1,
        ),
    ]
    metric.mock.incr.reset_mock()
    metric.mock.timing.reset_mock()

    hook_generator = metric.on_message_executed(data)
    await hook_generator.__anext__()
    with pytest.raises(StopAsyncIteration):
        await asyncio.sleep(1)
        await hook_generator.athrow(Exception())
    assert metric.mock.timing.await_args.args[0] == "message.executed"
    assert metric.mock.timing.await_args.args[1] >= 1
    assert metric.mock.timing.await_args.args[1] < 2
    assert metric.mock.timing.await_args.kwargs["params"] == pipeline_params
    assert metric.mock.incr.await_args_list == [
        call("message.executed.before", count=1, params=pipeline_params),
        call("message.executed.failed", count=1, params=pipeline_params),
    ]


@pytest.mark.asyncio
async def test_metrics_message_published(services_manager: ServicesManager) -> None:
    data = Mock()
    params = {"pipeline": data.xmsg.message.info.name, "topic": data.topic.name}
    metric = MockMetricsService(services_manager.services)

    hook_generator = metric.on_message_published(data)
    await hook_generator.asend(None)
    with pytest.raises(StopAsyncIteration):
        await hook_generator.asend(None)

    assert metric.mock.incr.await_args_list == [
        call("message.published.before", count=1, params=params),
        call("message.published.success", count=1, params=params),
    ]
    metric.mock.incr.reset_mock()

    hook_generator = metric.on_message_published(data)
    await hook_generator.asend(None)
    with pytest.raises(StopAsyncIteration):
        await hook_generator.athrow(Exception())

    assert metric.mock.incr.await_args_list == [
        call("message.published.before", count=1, params=params),
        call("message.published.failed", count=1, params=params),
    ]
    metric.mock.incr.reset_mock()
