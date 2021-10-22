import asyncio
from abc import abstractmethod
from collections.abc import Coroutine

from saturn_engine.core.message import Message
from saturn_engine.utils.log import getLogger

from ..queues import Processable


class Executor:
    @abstractmethod
    async def run(self) -> None:
        ...

    @abstractmethod
    async def submit(self, processable: Processable) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class BaseExecutor(Executor):
    def __init__(self, concurrency: int = 8) -> None:
        self.logger = getLogger(__name__, self)
        self.concurrency = concurrency
        self.queue: asyncio.Queue[Coroutine] = asyncio.Queue(maxsize=1)
        self.tasks: set[asyncio.Task] = set()

    async def run(self) -> None:
        get_task = None
        while True:
            if get_task is None and len(self.tasks) < self.concurrency:
                get_task = asyncio.create_task(self.queue.get())
                self.tasks.add(get_task)
            done, pending = await asyncio.wait(
                self.tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                self.tasks.discard(task)
                if task is get_task:
                    new_task = await get_task
                    self.tasks.add(asyncio.create_task(new_task))
                    get_task = None
                else:
                    exception = task.exception()
                    if exception:
                        print("error: ", str(exception))
                        self.logger.error(
                            "Executor '%s' failed", task, exc_info=exception
                        )

                    self.queue.task_done()

    async def submit(self, processable: Processable) -> None:
        processing_task = self.process(processable)
        await self.queue.put(processing_task)

    async def process(self, processable: Processable) -> None:
        async with processable.process() as message:
            await self.process_message(message)

    async def close(self) -> None:
        pass

    @abstractmethod
    async def process_message(self, message: Message) -> None:
        ...
