import typing as t

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


class ActorPool:
    def __init__(
        self,
        *,
        count: int,
        actor_cls: t.Type,
        actor_options: dict[str, t.Any],
        actor_args: tuple = (),
        actor_kwargs: t.Optional[dict[str, t.Any]] = None,
    ) -> None:
        self.count = count

        self.actor_cls = actor_cls
        self.actor_options = actor_options
        self.actor_args = actor_args
        self.actor_kwargs = actor_kwargs or {}

        self.actor_concurrency = self.actor_options.get("max_concurrency", 1)

        self.actors_slot: dict[int, int] = {}
        self.actors_queue: asyncio.PriorityQueue[
            tuple[int, int, t.Any]
        ] = asyncio.PriorityQueue()

        self.add(count)

    def add(self, count: int) -> None:
        for _ in range(count):
            actor = self.actor_cls.options(**self.actor_options).remote(
                *self.actor_args, **self.actor_kwargs
            )
            self.actors_slot[id(actor)] = 0
            for _ in range(self.actor_concurrency):
                self.push(actor)

    def push(self, actor: t.Any) -> None:
        actor_id = id(actor)
        self.actors_slot[actor_id] += 1
        self.actors_queue.put_nowait((-self.actors_slot[actor_id], actor_id, actor))

    async def pop(self) -> t.Any:
        _, actor_id, actor = await self.actors_queue.get()
        self.actors_slot[actor_id] -= 1
        return actor

    def empty(self) -> bool:
        return self.actors_queue.empty()

    @asynccontextmanager
    async def scoped_actor(self) -> AsyncIterator:
        try:
            actor = await self.pop()
            yield actor
        finally:
            self.push(actor)
