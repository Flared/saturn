import asyncio
import contextlib
import dataclasses
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from typing import Generic
from typing import TypeVar

from saturn_engine.utils.asyncutils import TasksGroup
from saturn_engine.utils.log import getLogger

T = TypeVar("T")


@dataclasses.dataclass(eq=False)
class Schedulable(Generic[T]):
    iterable: AsyncGenerator[T, None]
    # property for scheduling such as "weight" or priority would come here.


@dataclasses.dataclass
class ScheduleSlot(Generic[T]):
    generator: AsyncGenerator[T, None]
    task: asyncio.Task
    order: int = 0


class Scheduler(Generic[T]):
    schedule_slots: dict[Schedulable[T], ScheduleSlot[T]]
    tasks: dict[asyncio.Task, Schedulable[T]]

    def __init__(self) -> None:
        self.logger = getLogger(__name__, self)
        self.schedule_slots = {}
        self.tasks = {}
        self.tasks_group = TasksGroup()

    def add(self, item: Schedulable[T]) -> None:
        generator = item.iterable.__aiter__()
        task = asyncio.create_task(generator.__anext__())
        self.schedule_slots[item] = ScheduleSlot(task=task, generator=generator)
        self.tasks[task] = item
        self.tasks_group.add(task)

    async def remove(self, item: Schedulable[T]) -> None:
        schedule_slot = self.schedule_slots.pop(item, None)
        if schedule_slot is None:
            return
        await self.stop_slot(schedule_slot)

    async def close(self) -> None:
        cleanup_tasks = []

        await self.tasks_group.close()
        for item in self.schedule_slots.values():
            cleanup_tasks.append(self.stop_slot(item))

        await asyncio.gather(*cleanup_tasks)
        self.schedule_slots.clear()
        self.tasks.clear()

    async def stop_slot(self, schedule_slot: ScheduleSlot[T]) -> None:
        try:
            schedule_slot.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await schedule_slot.task
        except Exception:
            self.logger.exception("Failed to cancel item: %s", schedule_slot)

        try:
            await schedule_slot.generator.aclose()
        except Exception:
            self.logger.exception("Failed to close item: %s", schedule_slot)

    async def run(self) -> AsyncIterator[T]:
        while True:
            done = await self.tasks_group.wait()
            self.logger.debug("task ready: %s", done)

            for task in sorted(done, key=self.task_order):
                async for item in self.process_task(task):
                    yield item

    async def process_task(self, task: asyncio.Task) -> AsyncIterator[T]:
        item = self.tasks[task]
        del self.tasks[task]
        self.tasks_group.remove(task)

        try:
            # Even if the task finished, if the item was removed we
            # discard the item.
            if task.cancelled() or item not in self.schedule_slots:
                return

            exception = task.exception()
            if exception is None:
                yield task.result()
            elif isinstance(exception, StopAsyncIteration):
                await self.remove(item)
            elif isinstance(exception, asyncio.CancelledError):
                pass
            elif exception:
                self.logger.error("Failed to iter item item", exc_info=exception)
        except BaseException:
            # This is an unexpected error, likely a closed generator or
            # cancellation. The task is put back in the item for later
            # processing.
            self.tasks[task] = item
            self.tasks_group.add(task)
            raise
        else:
            # Requeue the __anext__ task to process next item.
            self._requeue_task(item)

    def _requeue_task(self, item: Schedulable[T]) -> None:
        schedule_slot = self.schedule_slots.get(item)
        if schedule_slot is None:
            return
        new_task = asyncio.create_task(schedule_slot.generator.__anext__())
        self.tasks[new_task] = item
        self.tasks_group.add(new_task)

        schedule_slot.task = new_task
        schedule_slot.order += 1

    def task_order(self, task: asyncio.Task) -> int:
        item = self.tasks[task]
        schedule_slot = self.schedule_slots.get(item)
        if schedule_slot is None:
            # Maximum priority so we clean the task as soon as possible.
            return -1
        return schedule_slot.order
