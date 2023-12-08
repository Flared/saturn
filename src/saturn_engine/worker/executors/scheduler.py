import typing as t

import asyncio
import dataclasses
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Coroutine

from saturn_engine.utils.asyncutils import TasksGroup
from saturn_engine.utils.log import getLogger

T = t.TypeVar("T")


class SchedulableProtocol(t.Protocol, t.Generic[T]):
    iterable: AsyncGenerator[T, None]
    name: str


@dataclasses.dataclass(eq=False)
class Schedulable(t.Generic[T]):
    iterable: AsyncGenerator[T, None]
    name: str


@dataclasses.dataclass
class ScheduleSlot(t.Generic[T]):
    generator: AsyncGenerator[T, None]
    task: asyncio.Task
    future: asyncio.Future[None]
    order: int = 0
    is_running: bool = True


class Scheduler(t.Generic[T]):
    schedule_slots: dict[SchedulableProtocol[T], ScheduleSlot[T]]
    tasks: dict[asyncio.Task, SchedulableProtocol[T]]

    def __init__(self) -> None:
        self.logger = getLogger(__name__, self)
        self.schedule_slots = {}
        self.tasks = {}
        self.tasks_group = TasksGroup()
        self.is_running: t.Optional[bool] = None

    def add(self, item: SchedulableProtocol[T]) -> asyncio.Future[None]:
        generator = t.cast(AsyncGenerator[T, None], item.iterable.__aiter__())
        name = f"scheduler.anext({item.name})"
        anext = t.cast(Coroutine[t.Any, t.Any, T], generator.__anext__())
        task = asyncio.create_task(anext, name=name)
        slot = ScheduleSlot(task=task, generator=generator, future=asyncio.Future())
        self.schedule_slots[item] = slot
        self.tasks[task] = item
        self.tasks_group.add(task)
        return slot.future

    def remove(self, item: SchedulableProtocol[T]) -> None:
        schedule_slot = self.schedule_slots.get(item)
        if schedule_slot:
            self.stop_slot(schedule_slot)

    async def close(self) -> None:
        self.is_running = False

        await self.tasks_group.close()
        for item in self.schedule_slots.values():
            self.stop_slot(item)

    def stop_slot(self, schedule_slot: ScheduleSlot[T]) -> None:
        schedule_slot.is_running = False
        if not schedule_slot.task.done():
            schedule_slot.task.cancel()

    async def stop_slot_generator(self, schedule_slot: ScheduleSlot[T]) -> None:
        try:
            self.logger.debug("Closing slot: %s", schedule_slot)
            await schedule_slot.generator.aclose()
        except Exception:
            self.logger.exception("Failed to close item: %s", schedule_slot)
        finally:
            schedule_slot.future.set_result(None)

    async def run(self) -> AsyncIterator[T]:
        if self.is_running is False:
            return

        self.is_running = True
        while self.is_running or self.tasks_group.tasks:
            done = await self.tasks_group.wait()
            if not done:
                continue
            self.logger.debug(
                "task ready",
                extra={"data": {"tasks": ",".join(t.get_name() for t in done)}},
            )

            for task in sorted(done, key=self.task_order):
                async for item in self.process_task(task):
                    yield item

    async def process_task(self, task: asyncio.Task) -> AsyncIterator[T]:
        item = self.tasks[task]
        del self.tasks[task]

        try:
            # Even if the task finished, if the item was removed we
            # discard the item.
            if task.cancelled() or item not in self.schedule_slots:
                return

            exception = task.exception()
            if exception is None:
                yield task.result()
            elif isinstance(exception, asyncio.CancelledError):
                pass
            elif isinstance(exception, Exception):
                self.remove(item)
                if not isinstance(exception, StopAsyncIteration):
                    self.logger.error(
                        "Exception raised from schedulable item",
                        extra={"data": {"queue_name": item.name}},
                        exc_info=exception,
                    )
            elif exception:
                raise ValueError("Fatal error in schedulable") from exception
        except BaseException:
            # This is an unexpected error, likely a closed generator or
            # cancellation. The task is put back in the item for later
            # processing.
            self.tasks[task] = item
            self.tasks_group.add(task)
            raise
        else:
            # Requeue the __anext__ task to process next item.
            schedule_slot = self.schedule_slots.get(item)
            if schedule_slot:
                if not schedule_slot.is_running:
                    del self.schedule_slots[item]
                    await self.stop_slot_generator(schedule_slot)
                else:
                    await self._requeue_task(item=item, schedule_slot=schedule_slot)

    async def _requeue_task(
        self, *, item: SchedulableProtocol[T], schedule_slot: ScheduleSlot[T]
    ) -> None:
        name = f"scheduler.anext({item.name})"
        anext = t.cast(Coroutine[t.Any, t.Any, T], schedule_slot.generator.__anext__())
        new_task = asyncio.create_task(anext, name=name)
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

    def __len__(self) -> int:
        return len(self.schedule_slots)

    def __bool__(self) -> int:
        return bool(self.schedule_slots)
