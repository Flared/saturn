import asyncio
from typing import Callable

import pytest

from saturn_engine.core.message import Message
from saturn_engine.worker.executors import BaseExecutor
from saturn_engine.worker.queues import Processable
from tests.utils import TimeForwardLoop


class FakeExecutor(BaseExecutor):
    def __init__(self) -> None:
        super().__init__(concurrency=5)
        self.start_executing = asyncio.Event()
        self.processing = 0
        self.processed = 0

    async def process_message(self, message: Message) -> None:
        self.processing += 1
        await self.start_executing.wait()
        self.processed += 1


@pytest.mark.asyncio
async def test_base_executor(
    processable_maker: Callable[[], Processable], event_loop: TimeForwardLoop
) -> None:
    executor = FakeExecutor()
    run_task = asyncio.create_task(executor.run())

    for _ in range(10):
        asyncio.create_task(executor.submit(processable_maker()))

    await event_loop.wait_idle()
    assert executor.processing == 5
    assert executor.processed == 0

    executor.start_executing.set()
    await event_loop.wait_idle()
    assert executor.processed == 10

    run_task.cancel()
    await executor.close()
