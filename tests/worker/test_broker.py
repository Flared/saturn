import typing as t

import asyncio
from unittest.mock import Mock

import pytest

from saturn_engine.config import Config
from saturn_engine.core import PipelineResults
from saturn_engine.core import api
from saturn_engine.core.api import InventoryItem
from saturn_engine.core.api import LockResponse
from saturn_engine.core.api import PipelineInfo
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import QueuePipeline
from saturn_engine.core.api import ResourceItem
from saturn_engine.utils.inspect import get_import_name
from saturn_engine.worker.broker import Broker
from saturn_engine.worker.executors import Executor
from saturn_engine.worker.pipeline_message import PipelineMessage
from tests.utils import register_hooks_handler
from tests.worker.conftest import FakeResource


class FakeExecutor(Executor):
    concurrency = 5
    done_event: asyncio.Event

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        pass

    async def process_message(self, message: PipelineMessage) -> PipelineResults:
        assert isinstance(message.message.args["resource"], dict)
        assert message.message.args["resource"]["data"] == "fake"
        if message.message.args["n"] == 999:
            FakeExecutor.done_event.set()
        return PipelineResults(outputs=[], resources=[])


def pipeline(resource: FakeResource) -> None:
    ...


@pytest.mark.asyncio
async def test_broker_dummy(
    broker_maker: t.Callable[..., Broker],
    config: Config,
    worker_manager_client: Mock,
) -> None:
    FakeExecutor.done_event = asyncio.Event()
    config = config.load_object(
        {"worker": {"executor_cls": get_import_name(FakeExecutor)}}
    )
    broker = broker_maker(config=config)

    hooks_handler = register_hooks_handler(broker.services_manager.services)
    pipeline_info = PipelineInfo.from_pipeline(pipeline)
    worker_manager_client.lock.return_value = LockResponse(
        items=[
            QueueItem(
                name="j1",
                input=InventoryItem(
                    name="dummy", type="DummyInventory", options={"count": 10000}
                ),
                pipeline=QueuePipeline(args={}, info=pipeline_info),
                output={},
            )
        ],
        resources=[
            ResourceItem(
                name="r1",
                type="FakeResource",
                data={"data": "fake"},
            ),
        ],
        executors=[
            api.Executor(
                name="e1", type=get_import_name(FakeExecutor), options={"assert": True}
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

    broker_task.cancel()
    await broker_task
