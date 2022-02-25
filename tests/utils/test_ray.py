import random

import pytest

from saturn_engine.utils.ray import ActorPool


@pytest.mark.asyncio
async def test_actor_pool(ray_cluster: None) -> None:
    import ray

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
