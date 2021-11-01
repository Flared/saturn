import asyncio
from unittest.mock import Mock

import pytest

from saturn_engine.core.api import DummyJob
from saturn_engine.core.api import Inventory
from saturn_engine.core.api import SyncResponse
from saturn_engine.worker.broker import Broker
from saturn_engine.worker.queues import Processable


@pytest.mark.asyncio
async def test_broker_dummy(broker: Broker, worker_manager_client: Mock) -> None:
    done_event = asyncio.Event()

    async def fake_executor(processable: Processable) -> None:
        async with processable.process() as message:
            if message.body == "999":
                done_event.set()

    broker.executor.submit.side_effect = fake_executor  # type: ignore[attr-defined]
    worker_manager_client.sync.return_value = SyncResponse(
        items=[
            DummyJob(
                id="j1",
                pipeline="p1",
                ressources=[],
                inventory=Inventory(
                    name="dummy", type="dummy", options={"count": 1000}
                ),
                options={},
            )
        ]
    )

    wait_task = asyncio.create_task(done_event.wait())
    broker_task = asyncio.create_task(broker.run())
    tasks: set[asyncio.Task] = {wait_task, broker_task}
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    assert wait_task in done
    assert broker_task in pending
