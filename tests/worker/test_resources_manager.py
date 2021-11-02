import asyncio

import pytest

from saturn_engine.worker.resources_manager import ResourceData
from saturn_engine.worker.resources_manager import ResourcesManager
from saturn_engine.worker.resources_manager import ResourceUnavailable
from tests.utils import TimeForwardLoop


@pytest.mark.asyncio
async def test_resources_manager_acquire() -> None:
    resources_manager = ResourcesManager()

    r1 = ResourceData(id="r1", type="R1", data={})
    r2 = ResourceData(id="r2", type="R1", data={})
    r3 = ResourceData(id="r3", type="R2", data={})

    await resources_manager.add(r1)
    await resources_manager.add(r2)
    await resources_manager.add(r3)

    with pytest.raises(ValueError):
        await resources_manager.add(r3)

    resource1 = await resources_manager.acquire("R1", wait=True)
    resource2 = await resources_manager.acquire("R1", wait=False)
    with pytest.raises(ResourceUnavailable):
        await resources_manager.acquire("R1", wait=False)

    async with resource1 as locked_r1:
        async with resource2 as locked_r2:
            assert {locked_r1, locked_r2} == {r1, r2}
            assert locked_r1 not in {locked_r2, r3}

        # r2 should be released, lock again.
        resource3 = await resources_manager.acquire("R1")
        async with resource3 as locked_r3:
            assert {locked_r1, locked_r3} == {r1, r2}
            assert locked_r1 not in {locked_r3, r3}


@pytest.mark.asyncio
async def test_resources_manager_acquire_many(event_loop: TimeForwardLoop) -> None:
    r1 = ResourceData(id="r1", type="R1", data={})
    r2 = ResourceData(id="r2", type="R2", data={})
    r3 = ResourceData(id="r3", type="R3", data={})
    r4 = ResourceData(id="r4", type="R4", data={})

    resources_manager = ResourcesManager()

    await resources_manager.add(r1)
    await resources_manager.add(r2)
    await resources_manager.add(r3)
    await resources_manager.add(r4)

    resource1 = await resources_manager.acquire_many(["R1", "R2"], wait=False)
    with pytest.raises(ResourceUnavailable):
        await resources_manager.acquire_many(["R2", "R3"], wait=False)
    with pytest.raises(ResourceUnavailable):
        await resources_manager.acquire_many(["R4", "R1"], wait=False)
    resource4 = await resources_manager.acquire_many(["R3", "R4"], wait=False)

    async with resource1 as locked_r1:
        assert locked_r1["R1"] is r1
        assert locked_r1["R2"] is r2

    async with resource4 as locked_r4:
        assert locked_r4["R3"] is r3
        assert locked_r4["R4"] is r4

    # Check for philosopher diner.
    # Philosopher 1 lock r1 and r2.
    # Philosopher 2 lock r2 and r3
    # Philosopher 3 lock r3 and r1
    philosophers = {
        asyncio.create_task(resources_manager.acquire_many(["R1", "R2"])),
        asyncio.create_task(resources_manager.acquire_many(["R2", "R3"])),
        asyncio.create_task(resources_manager.acquire_many(["R3", "R4"])),
        asyncio.create_task(resources_manager.acquire_many(["R4", "R1"])),
    }
    await event_loop.wait_idle()

    # All resource should be locked.
    for i in range(1, 5):
        with pytest.raises(ResourceUnavailable):
            await resources_manager.acquire(f"R{i}", wait=False)

    while philosophers:
        done, pending = await asyncio.wait(
            philosophers, return_when=asyncio.FIRST_COMPLETED
        )
        philosophers = pending
        for philosopher in done:
            async with philosopher.result() as resources:
                assert len(resources) == 2
