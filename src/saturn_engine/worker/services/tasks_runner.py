import typing as t

import asyncio

from saturn_engine.utils.asyncutils import TasksGroupRunner


class TasksRunnerService:
    name = "tasks_runner"

    def __init__(self) -> None:
        self.runner = TasksGroupRunner(name="tasks-runner-service")
        self.runner.start()

    async def close(self) -> None:
        await self.runner.close()

    def create_task(self, coro: t.Coroutine, *, name: str) -> asyncio.Task:
        return self.runner.create_task(coro, name=name)
