import asyncio
import contextlib
from collections.abc import Coroutine
from typing import Union

from saturn_engine.utils.log import getLogger


class TaskManager:
    def __init__(self) -> None:
        self.logger = getLogger(__name__, self)
        self.tasks: set[asyncio.Task] = set()
        self.tasks_updated = asyncio.Event()

    def add(self, task: Union[asyncio.Task, Coroutine]) -> asyncio.Task:
        if isinstance(task, Coroutine):
            task = asyncio.create_task(task)
        if not isinstance(task, asyncio.Task):
            raise ValueError("Expected asyncio.Task or Coroutine")
        self.tasks.add(task)
        self.tasks_updated.set()
        return task

    def remove(self, task: asyncio.Task) -> None:
        task.cancel()
        self.tasks.discard(task)
        self.tasks_updated.set()

    async def run(self) -> None:
        tasks_updated = asyncio.create_task(self.tasks_updated.wait())
        try:
            while tasks_updated:
                done, pending = await asyncio.wait(
                    self.tasks | {tasks_updated}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    if task is tasks_updated:
                        self.tasks_updated.clear()
                        tasks_updated = asyncio.create_task(self.tasks_updated.wait())
                    else:
                        self.on_done(task)

        except asyncio.CancelledError:
            tasks_updated.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await tasks_updated

    def on_done(self, task: asyncio.Task) -> None:
        exception = task.exception()
        if exception:
            self.logger.error("Task '%s' failed", task, exc_info=exception)
        else:
            self.logger.warning(
                "Task '%s' completed with result: %s", task, task.result()
            )
        self.tasks.discard(task)

    async def close(self) -> None:
        for task in self.tasks:
            task.cancel()
        tasks_results = await asyncio.gather(*self.tasks, return_exceptions=True)
        for result in tasks_results:
            if isinstance(result, Exception):
                self.logger.error(
                    "Task '%s' cancelled with error", task, exc_info=result
                )
