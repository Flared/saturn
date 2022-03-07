import random

import pytest


@pytest.mark.asyncio
async def test_actor_pool(ray_cluster: None) -> None:
    import ray

    from saturn_engine.utils.ray import ActorPool

    @ray.remote
    class Actor:
        def __init__(self, arg: int, *, kwarg: int) -> None:
            self.arg = arg
            self.kwarg = kwarg

        def call(self) -> int:
            return id(self)

    pool = ActorPool(
        count=2,
        actor_cls=Actor,
        actor_options={"max_concurrency": 2},
        actor_args=(1,),
        actor_kwargs={"kwarg": 1},
    )

    assert not pool.empty()
    a1 = await pool.pop()
    assert not pool.empty()

    a2 = await pool.pop()
    assert not pool.empty()
    assert a1 is not a2

    a3 = await pool.pop()
    assert not pool.empty()
    assert a3 in (a1, a2)

    a4 = await pool.pop()
    assert pool.empty()
    assert a4 is not a3
    assert a4 in (a1, a2)

    pool.push(a1)
    assert not pool.empty()
    a1_again = await pool.pop()
    assert a1_again is a1

    # Test multiple time to ensure we are not just lucky.
    # If we push a1 twice and a2 once, we should be guarenteed to get a1 back.
    for _ in range(50):
        actors = [a1, a1, a2]
        random.shuffle(actors)
        for a in actors:
            pool.push(a)
        a = await pool.pop()
        assert a is a1
        await pool.pop()
        await pool.pop()

    pool.push(a1)
    async with pool.scoped_actor() as actor:
        assert actor is a1
        assert pool.empty()
    assert not pool.empty()


@pytest.mark.asyncio
async def test_actor_pool_removed(remote_ray_cluster: None) -> None:
    import ray
    from ray.exceptions import RayActorError

    from saturn_engine.utils.ray import ActorPool

    @ray.remote
    class Actor:
        def call(self) -> int:
            return 0

    pool = ActorPool(
        count=2,
        actor_cls=Actor,
        actor_options={"max_concurrency": 2},
        actor_args=(),
        actor_kwargs={},
    )

    a1 = await pool.pop()
    a2 = await pool.pop()

    with pytest.raises(RayActorError):
        async with pool.scoped_actor() as actor:
            # Swap actors so a1 is actor
            if a2 is actor:
                a1, a2 = a2, a1

            ray.kill(actor)
            await actor.call.remote()

    assert a1 not in pool
    assert actor not in pool
    assert a2 in pool
    assert len(pool) == 2

    # Trying to put them back won't change the pool.
    pool.push(a1)
    pool.push(actor)
    assert a1 not in pool
    assert actor not in pool
    assert len(pool) == 2

    # We also won't pop dead actor, even if they were still in queue.
    pool.push(a2)
    with pytest.raises(RayActorError):
        async with pool.scoped_actor() as actor:
            dead_actor = actor
            ray.kill(actor)
            await actor.call.remote()
    while not pool.empty():
        assert dead_actor is not await pool.pop()
