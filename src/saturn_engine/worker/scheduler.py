import asyncio
import contextlib
import dataclasses
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator

from saturn_engine.utils.log import getLogger

from .queues import BlockingQueue
from .queues import Processable
from .queues import Queue


@dataclasses.dataclass
class QueueSlot:
    generator: AsyncGenerator[Processable, None]
    task: asyncio.Task
    order: int = 0


class Scheduler:
    queues: dict[Queue, QueueSlot]
    tasks_queues: dict[asyncio.Task, BlockingQueue]

    def __init__(self) -> None:
        self.logger = getLogger(__name__, self)
        self.queues = {}
        self.tasks_queues = {}

    def add(self, queue: Queue) -> None:
        task_queue = BlockingQueue(queue)
        generator = task_queue.run().__aiter__()
        task = asyncio.create_task(generator.__anext__())
        self.queues[queue] = QueueSlot(task=task, generator=generator)
        self.tasks_queues[task] = task_queue

    async def remove(self, queue: Queue) -> None:
        queue_slot = self.queues.pop(queue, None)
        if queue_slot is None:
            return
        await self.stop_queue(queue_slot)

    async def close(self) -> None:
        cleanup_tasks = []

        for queue in self.queues.values():
            cleanup_tasks.append(self.stop_queue(queue))

        await asyncio.gather(*cleanup_tasks)
        self.queues.clear()
        self.tasks_queues.clear()

    async def stop_queue(self, queue_slot: QueueSlot) -> None:
        try:
            queue_slot.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await queue_slot.task
        except Exception:
            self.logger.exception("Failed to cancel queue: %s", queue_slot)

        try:
            await queue_slot.generator.aclose()
        except Exception:
            self.logger.exception("Failed to close queue: %s", queue_slot)

    async def run(self) -> AsyncIterator[Processable]:
        while True:
            if not self.tasks_queues:
                await asyncio.sleep(1)
                continue

            done, pending = await asyncio.wait(
                self.tasks_queues.keys(), return_when=asyncio.FIRST_COMPLETED
            )

            self.logger.debug("task ready: %s", done)

            for task in sorted(done, key=self.task_order):
                async for item in self.process_task(task):
                    yield item

    async def process_task(self, task: asyncio.Task) -> AsyncIterator[Processable]:
        queue = self.tasks_queues[task]
        del self.tasks_queues[task]

        try:
            # Even if the task finished, if the queue was removed we
            # discard the item.
            if task.cancelled() or queue.queue not in self.queues:
                return

            exception = task.exception()
            if exception is None:
                yield task.result()
            elif isinstance(exception, StopAsyncIteration):
                await self.remove(queue.queue)
            elif isinstance(exception, asyncio.CancelledError):
                pass
            elif exception:
                self.logger.error("Failed to iter queue item", exc_info=exception)
        except BaseException:
            # This is an unexpected error, likely a closed generator or
            # cancellation. The task is put back in the queue for later
            # processing.
            self.tasks_queues[task] = queue
            raise
        else:
            # Requeue the __anext__ task to process next item.
            self._requeue_queue_task(queue)

    def _requeue_queue_task(self, queue: BlockingQueue) -> None:
        queue_slot = self.queues.get(queue.queue)
        if queue_slot is None:
            return
        new_task = asyncio.create_task(queue_slot.generator.__anext__())
        self.tasks_queues[new_task] = queue
        queue_slot.task = new_task
        queue_slot.order += 1

    def task_order(self, task: asyncio.Task) -> int:
        queue = self.tasks_queues[task]
        queue_slot = self.queues.get(queue.queue)
        if queue_slot is None:
            # Maximum priority so we clean the task as soon as possible.
            return -1
        return queue_slot.order
