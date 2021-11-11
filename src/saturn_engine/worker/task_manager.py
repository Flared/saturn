import asyncio
from collections.abc import Coroutine
from typing import Union

from saturn_engine.utils.asyncutils import TasksGroup
from saturn_engine.utils.log import getLogger


class TaskManager:
    def __init__(self) -> None:
        self.logger = getLogger(__name__, self)
        self.tasks = TasksGroup()

    def add(self, task: Union[asyncio.Task, Coroutine]) -> asyncio.Task:
        if isinstance(task, Coroutine):
            task = asyncio.create_task(task)
        if not isinstance(task, asyncio.Task):
            raise ValueError("Expected asyncio.Task or Coroutine")
        self.tasks.add(task)
        return task

    def remove(self, task: asyncio.Task) -> None:
        task.cancel()
        self.tasks.remove(task)

    async def run(self) -> None:
        while True:
            done = await self.tasks.wait()
            for task in done:
                self.on_done(task)

    def on_done(self, task: asyncio.Task) -> None:
        exception = task.exception()
        if exception:
            self.logger.error("Task '%s' failed", task, exc_info=exception)
        else:
            self.logger.warning(
                "Task '%s' completed with result: %s", task, task.result()
            )
        self.tasks.remove(task)

    async def close(self) -> None:
        await self.tasks.close()
