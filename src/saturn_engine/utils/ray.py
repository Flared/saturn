import typing as t

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from ray.exceptions import RayActorError


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
        self._start_actors()

    def _start_actors(self) -> None:
        for _ in range(self.count - len(self.actors_slot)):
            self._start_actor()

    def _start_actor(self) -> None:
        actor = self.actor_cls.options(**self.actor_options).remote(
            *self.actor_args, **self.actor_kwargs
        )
        self.actors_slot[id(actor)] = 0
        for _ in range(self.actor_concurrency):
            self.push(actor)

    def _restart_actor(self, actor: t.Any) -> None:
        self.actors_slot.pop(id(actor), None)
        self._start_actors()

    def push(self, actor: t.Any) -> None:
        actor_id = id(actor)
        if actor_id not in self.actors_slot:
            return

        self.actors_slot[actor_id] += 1
        self.actors_queue.put_nowait((-self.actors_slot[actor_id], actor_id, actor))

    async def pop(self) -> t.Any:
        while True:
            _, actor_id, actor = await self.actors_queue.get()
            if actor_id not in self.actors_slot:
                continue
            self.actors_slot[actor_id] -= 1
            return actor

    def empty(self) -> bool:
        return self.actors_queue.empty()

    @asynccontextmanager
    async def scoped_actor(self) -> AsyncIterator:
        release = True
        try:
            actor = await self.pop()
            yield actor
        except RayActorError:
            release = False
            self._restart_actor(actor)
            raise
        finally:
            if release:
                self.push(actor)

    def close(self) -> None:
        self.actors_slot.clear()
        while not self.actors_queue.empty():
            self.actors_queue.get_nowait()

    def __len__(self) -> int:
        return len(self.actors_slot)

    def __contains__(self, actor: t.Any) -> int:
        return id(actor) in self.actors_slot
