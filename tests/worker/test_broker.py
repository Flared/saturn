import asyncio
from unittest.mock import Mock

import pytest

from saturn_engine.core import PipelineMessage
from saturn_engine.core.api import DummyJob
from saturn_engine.core.api import Inventory
from saturn_engine.core.api import PipelineInfo
from saturn_engine.core.api import QueuePipeline
from saturn_engine.core.api import SyncResponse
from saturn_engine.worker.broker import Broker
from saturn_engine.worker.executors import BaseExecutor


class FakeExecutor(BaseExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.done_event = asyncio.Event()

    async def process_message(self, message: PipelineMessage) -> None:
        if message.message.args == {"n": 999}:
            self.done_event.set()


@pytest.mark.asyncio
async def test_broker_dummy(
    broker: Broker, worker_manager_client: Mock, fake_pipeline_info: PipelineInfo
) -> None:
    executor = FakeExecutor()
    broker.executor = executor
    worker_manager_client.sync.return_value = SyncResponse(
        items=[
            DummyJob(
                id="j1",
                pipeline=QueuePipeline(args={}, info=fake_pipeline_info),
                inventory=Inventory(
                    name="dummy", type="dummy", options={"count": 10000}
                ),
                options={},
            )
        ]
    )

    wait_task = asyncio.create_task(executor.done_event.wait())
    broker_task = asyncio.create_task(broker.run())
    tasks: set[asyncio.Task] = {wait_task, broker_task}
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    assert wait_task in done
    assert broker_task in pending
    broker_task.cancel()
