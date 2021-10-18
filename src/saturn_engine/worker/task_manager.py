import asyncio


class TaskManager:
    tasks: list[asyncio.Task]

    def __init__(self) -> None:
        self.tasks = []

    def add(self, task: asyncio.Task) -> None:
        pass

    def remove(self, task: asyncio.Task) -> None:
        pass

    async def close(self) -> None:
        pass
