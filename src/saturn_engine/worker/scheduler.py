import asyncio
import dataclasses
from typing import AsyncGenerator

from saturn_engine.core import Message
from saturn_engine.utils.log import getLogger

from .queues import Queue


@dataclasses.dataclass
class QueueSlot:
    iterator: AsyncGenerator[Message, None]
    task: asyncio.Task
    order: int = 0


class Scheduler:
    queues: dict[Queue, QueueSlot]
    tasks_queues: dict[asyncio.Task, Queue]

    def __init__(self) -> None:
        self.logger = getLogger(__name__, self)
        self.queues = {}
        self.tasks_queues = {}

    def add(self, queue: Queue) -> None:
        iterator = queue.iterator().__aiter__()
        task = asyncio.create_task(iterator.__anext__())
        self.queues[queue] = QueueSlot(task=task, iterator=iterator)
        self.tasks_queues[task] = queue

    async def remove(self, queue: Queue) -> None:
        queue_slot = self.queues.pop(queue, None)
        if queue_slot is None:
            return
        await self.stop_queue(queue_slot)

    async def close(self) -> None:
        cleanup_tasks = []

        async def clean_queue(queue: QueueSlot) -> None:
            try:
                await self.stop_queue(queue)
                await queue.task
            except Exception:
                self.logger.exception("Failed to clean queue: %s", queue)

        for queue in self.queues.values():
            cleanup_tasks.append(clean_queue(queue))

        await asyncio.gather(*cleanup_tasks)
        self.queues.clear()
        self.tasks_queues.clear()

    async def stop_queue(self, queue_slot: QueueSlot) -> None:
        queue_slot.task.cancel()
        try:
            await queue_slot.iterator.aclose()
        except GeneratorExit:
            pass

    async def iter(self) -> AsyncGenerator[Message, None]:
        while True:
            if not self.tasks_queues:
                await asyncio.sleep(1)
                continue

            done, pending = await asyncio.wait(
                self.tasks_queues.keys(), return_when=asyncio.FIRST_COMPLETED
            )

            for task in sorted(done, key=self.task_order):
                queue = self.tasks_queues[task]
                del self.tasks_queues[task]

                try:
                    # Even if the task finished, if the queue was removed we
                    # discard the item.
                    if task.cancelled() or queue not in self.queues:
                        continue

                    exception = task.exception()
                    if exception is None:
                        yield task.result()
                    elif isinstance(exception, StopAsyncIteration):
                        await self.remove(queue)
                    elif exception:
                        self.logger.error(
                            "Failed to iter queue item", exc_info=exception
                        )
                except BaseException:
                    # This is an unexpected error, likely a closed generator or
                    # cancellation. The task is put back in the queue for later
                    # processing.
                    self.tasks_queues[task] = queue
                    raise

                self._requeue_queue_task(queue)

    def _requeue_queue_task(self, queue: Queue) -> None:
        queue_slot = self.queues.get(queue)
        if queue_slot is None:
            return
        new_task = asyncio.create_task(queue_slot.iterator.__anext__())
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
