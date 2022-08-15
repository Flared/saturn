from saturn_engine.utils.asyncutils import TasksGroupRunner

from . import MinimalService


class TasksRunnerService(MinimalService):
    name = "tasks_runner"

    async def open(self) -> None:
        self.runner = TasksGroupRunner(name="tasks-runner-service")
        self.runner.start()

    async def close(self) -> None:
        await self.runner.close()
