import asyncio
import time

import pytest
from limits.aio import storage

from saturn_engine.worker.resources.manager import ResourceData
from saturn_engine.worker.resources.manager import ResourceRateLimit
from saturn_engine.worker.resources.manager import ResourcesManager
from saturn_engine.worker.resources.manager import ResourceUnavailable
from tests.utils import TimeForwardLoop


@pytest.mark.asyncio
async def test_resources_manager_acquire() -> None:
    resources_manager = ResourcesManager()

    r1 = ResourceData(name="r1", type="R1", data={})
    r2 = ResourceData(name="r2", type="R1", data={})
    r3 = ResourceData(name="r3", type="R2", data={})

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
            assert {locked_r1.resource, locked_r2.resource} == {r1, r2}
            assert locked_r1.resource not in {locked_r2.resource, r3}

        # r2 should be released, lock again.
        resource3 = await resources_manager.acquire("R1")
        async with resource3 as locked_r3:
            assert {locked_r1.resource, locked_r3.resource} == {r1, r2}
            assert locked_r1.resource not in {locked_r3.resource, r3}


@pytest.mark.asyncio
async def test_resources_manager_acquire_many(event_loop: TimeForwardLoop) -> None:
    r1 = ResourceData(name="r1", type="R1", data={})
    r2 = ResourceData(name="r2", type="R2", data={})
    r3 = ResourceData(name="r3", type="R3", data={})
    r4 = ResourceData(name="r4", type="R4", data={})

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
        assert locked_r1["R1"].resource is r1
        assert locked_r1["R2"].resource is r2

    async with resource4 as locked_r4:
        assert locked_r4["R3"].resource is r3
        assert locked_r4["R4"].resource is r4

    # Check for philosopher diner.
    # Philosopher 1 lock r1 and r2.
    # Philosopher 2 lock r2 and r3
    # Philosopher 3 lock r3 and r1
    async with event_loop.until_idle():
        philosophers = {
            asyncio.create_task(resources_manager.acquire_many(["R1", "R2"])),
            asyncio.create_task(resources_manager.acquire_many(["R2", "R3"])),
            asyncio.create_task(resources_manager.acquire_many(["R3", "R4"])),
            asyncio.create_task(resources_manager.acquire_many(["R4", "R1"])),
        }

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


@pytest.mark.asyncio
async def test_resources_manager_release_later(event_loop: TimeForwardLoop) -> None:
    r1 = ResourceData(name="r1", type="R", data={})
    resources_manager = ResourcesManager()
    await resources_manager.add(r1)

    resource = await resources_manager.acquire("R", wait=False)
    async with resource:
        resource.release_later(time.time() + 1)

    await asyncio.sleep(0.5)

    with pytest.raises(ResourceUnavailable):
        await resources_manager.acquire("R", wait=False)

    await asyncio.sleep(1)
    await resources_manager.acquire("R", wait=False)


@pytest.mark.asyncio
async def test_resources_manager_default_delay(event_loop: TimeForwardLoop) -> None:
    r1 = ResourceData(name="r1", type="R", data={}, default_delay=1)
    resources_manager = ResourcesManager()
    await resources_manager.add(r1)

    resource = await resources_manager.acquire("R", wait=False)
    async with resource:
        pass

    await asyncio.sleep(0.5)

    with pytest.raises(ResourceUnavailable):
        await resources_manager.acquire("R", wait=False)

    await asyncio.sleep(1)
    await resources_manager.acquire("R", wait=False)


@pytest.mark.asyncio
async def test_resources_manager_with_hourly_rate_limiter(
    event_loop: TimeForwardLoop,
) -> None:
    r1 = ResourceData(
        name="r1",
        type="R",
        data={},
        rate_limit=ResourceRateLimit(
            rate_limits=["3 per hour"], strategy="moving-window"
        ),
    )
    resources_manager = ResourcesManager()
    await resources_manager.add(r1)

    resource = await resources_manager.acquire("R", wait=False)
    async with resource:
        pass

    resource = await resources_manager.acquire("R", wait=False)
    async with resource:
        pass

    resource = await resources_manager.acquire("R", wait=False)
    async with resource:
        pass

    await asyncio.sleep(1)

    # this one should fail
    with pytest.raises(ResourceUnavailable):
        resource = await resources_manager.acquire("R", wait=False)
        async with resource:
            pass

    # after one hour, should be able to acquire resource
    await asyncio.sleep(3600)

    resource = await resources_manager.acquire("R", wait=False)
    async with resource:
        pass


@pytest.mark.asyncio
async def test_resources_manager_with_daily_rate_limiter(
    event_loop: TimeForwardLoop,
) -> None:
    r1 = ResourceData(
        name="r1",
        type="R",
        data={},
        rate_limit=ResourceRateLimit(
            rate_limits=["2 per day"], strategy="moving-window"
        ),
    )
    resources_manager = ResourcesManager()
    await resources_manager.add(r1)

    resource = await resources_manager.acquire("R", wait=False)
    async with resource:
        pass

    resource = await resources_manager.acquire("R", wait=False)
    async with resource:
        pass

    # this one should fail
    with pytest.raises(ResourceUnavailable):
        await resources_manager.acquire("R", wait=False)

    # after one hour, it should be still unavailable
    await asyncio.sleep(3600)

    with pytest.raises(ResourceUnavailable):
        await resources_manager.acquire("R", wait=False)

    # wait 23 hours, should be availabe
    await asyncio.sleep((23 * 3600))

    resource = await resources_manager.acquire("R", wait=False)
    async with resource:
        pass

    await asyncio.sleep(2 * 3600)

    resource = await resources_manager.acquire("R", wait=False)
    async with resource:
        pass

    # after 22 hours, we should be able to do one request
    await asyncio.sleep((22 * 3600) + 1)

    resource = await resources_manager.acquire("R", wait=False)
    async with resource:
        pass

    with pytest.raises(ResourceUnavailable):
        await resources_manager.acquire("R", wait=False)

    # one request should be available after 2 hours
    await asyncio.sleep(2 * 3600)

    resource = await resources_manager.acquire("R", wait=False)
    async with resource:
        pass

    await asyncio.sleep(24 * 3600)


@pytest.mark.asyncio
async def test_resources_manager_with_hourly_rate_limit_and_wait(
    event_loop: TimeForwardLoop,
) -> None:
    r1 = ResourceData(
        name="r1",
        type="R",
        data={},
        rate_limit=ResourceRateLimit(
            rate_limits=["1 per hour"], strategy="moving-window"
        ),
    )
    resources_manager = ResourcesManager()
    await resources_manager.add(r1)

    resource = await resources_manager.acquire("R", wait=False)
    async with resource:
        pass

    with pytest.raises(ResourceUnavailable):
        resource = await resources_manager.acquire("R", wait=False)

    time_start = int(time.time())

    resource = await resources_manager.acquire("R", wait=True)
    async with resource:
        pass

    assert (int(time.time()) - time_start) == 3600


def test_resource_manager_with_redis_dsn_create_redis_storage(
    event_loop: TimeForwardLoop,
) -> None:
    resources_manager = ResourcesManager(redis_dsn="redis://localhost:6379")
    assert isinstance(resources_manager.limiters_storage, storage.RedisStorage)


def test_resource_manager_without_redis_dsn_create_memory_storage(
    event_loop: TimeForwardLoop,
) -> None:
    resources_manager = ResourcesManager()
    assert isinstance(resources_manager.limiters_storage, storage.MemoryStorage)
