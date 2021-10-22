import asyncio
from collections.abc import AsyncIterator

import pytest

from saturn_engine.worker.task_manager import TaskManager


@pytest.fixture
async def task_manager() -> AsyncIterator[TaskManager]:
    _task_manager = TaskManager()
    yield _task_manager
    await _task_manager.close()


@pytest.mark.asyncio
async def test_task_manager(  # noqa: C901  # Ignore complexity errors.
    task_manager: TaskManager,
) -> None:
    task_ran = asyncio.Event()

    async def task_forever() -> None:
        while True:
            task_ran.set()
            await asyncio.sleep(0)

    async def task_complete() -> None:
        await asyncio.sleep(0)

    async def task_error() -> None:
        await asyncio.sleep(0)
        raise ValueError()

    async def task_cancel_error() -> None:
        try:
            while True:
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise ValueError() from None

    forever_task = asyncio.create_task(task_forever())
    task_manager.add(forever_task)
    task_manager.add(asyncio.create_task(task_complete()))
    task_manager.add(asyncio.create_task(task_error()))
    cancel_task = asyncio.create_task(task_cancel_error())
    task_manager.add(cancel_task)
    run_task = asyncio.create_task(task_manager.run())
    wait_task = asyncio.create_task(task_ran.wait())

    # Running all tasks, even if some complete or fail, other task are awaited and run.
    tasks: set[asyncio.Task] = {run_task, wait_task}
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    assert wait_task in done
    assert run_task in pending

    new_task_ran = asyncio.Event()

    async def new_task() -> None:
        while True:
            new_task_ran.set()
            await asyncio.sleep(0)

    # Adding a new task adds it to the list and is awaited.
    new_forever_task = asyncio.create_task(new_task())
    task_manager.add(new_forever_task)
    wait_new_task = asyncio.create_task(new_task_ran.wait())
    tasks = {run_task, wait_new_task}
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    assert wait_new_task in done
    assert run_task in pending

    async def removed() -> None:
        while True:
            await asyncio.sleep(0)
            task_manager.remove(removed_task)

    # Removing a task cancel it.
    removed_task = asyncio.create_task(removed())
    task_manager.add(removed_task)
    tasks = {run_task, removed_task}
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    assert removed_task in done
    assert run_task in pending

    run_task.cancel()
    await run_task
    # The task still running at the end are the ones without error or completion.
    assert task_manager.tasks == {forever_task, new_forever_task, cancel_task}
