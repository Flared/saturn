import asyncio
import dataclasses
from typing import AsyncGenerator

from saturn.core import Message

from .queues import Queue


@dataclasses.dataclass
class QueueSlot:
    task: asyncio.Task
    order: int = 0


class Scheduler:
    queues: dict[Queue, QueueSlot]
    tasks_queues: dict[asyncio.Task, Queue]

    def __init__(self) -> None:
        self.queues = {}
        self.tasks_queues = {}

    def add(self, queue: Queue) -> None:
        task = asyncio.create_task(queue.get())
        self.queues[queue] = QueueSlot(task=task)
        self.tasks_queues[task] = queue

    def remove(self, queue: Queue) -> None:
        task = self.queues.pop(queue).task
        task.cancel()

    async def iter(self) -> AsyncGenerator[Message, None]:
        while True:
            if not self.tasks_queues:
                await asyncio.sleep(1)
                continue

            done, pending = await asyncio.wait(
                self.tasks_queues.keys(), return_when=asyncio.FIRST_COMPLETED
            )

            for task in sorted(done, key=self.task_order):
                if not task.cancelled():
                    yield await task

                # Create a new queue task.
                queue = self.tasks_queues[task]
                del self.tasks_queues[task]

                queue_slot = self.queues.get(queue)
                if queue_slot is None:
                    continue
                new_task = asyncio.create_task(queue.get())
                self.tasks_queues[new_task] = queue
                queue_slot.task = new_task
                queue_slot.order += 1

    def task_order(self, task: asyncio.Task) -> int:
        queue = self.tasks_queues[task]
        queue_slot = self.queues.get(queue)
        if queue_slot is None:
            # Maximum priority so we clean the task as soon as possible.
            return -1
        return self.queues[queue].order
