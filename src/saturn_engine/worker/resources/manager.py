import typing as t

import asyncio
import contextlib
import dataclasses
import time
from collections import defaultdict
from collections.abc import Iterable
from functools import cached_property

from limits import RateLimitItem
from limits import parse_many
from limits.aio import storage
from limits.aio.strategies import STRATEGIES
from limits.aio.strategies import RateLimiter


@dataclasses.dataclass(frozen=True)
class ResourceKey:
    name: str
    type: str


@dataclasses.dataclass(frozen=True)
class ResourceRateLimit:
    rate_limits: list[str]
    strategy: str = "fixed-window"

    @cached_property
    def rate_limit_items(self) -> list[RateLimitItem]:
        return parse_many(";".join(self.rate_limits))


@dataclasses.dataclass(eq=False)
class ResourceData:
    name: str
    type: str
    data: dict[str, object]
    default_delay: float = 0
    rate_limit: t.Optional[ResourceRateLimit] = None
    state: t.Optional[dict[str, object]] = None

    @property
    def key(self) -> ResourceKey:
        return ResourceKey(name=self.name, type=self.type)


class ResourceUnavailable(Exception):
    pass


class ResourceContext:
    def __init__(self, resource: ResourceData, manager: "ExclusiveResources") -> None:
        self.resource: t.Optional[ResourceData] = resource
        self.manager = manager
        self.rate_limiter: t.Optional[RateLimiter] = manager.limiters.get(resource.name)
        self.release_at: t.Optional[float] = None

    async def release(self) -> None:
        # Add the resource back to the manager.
        if not self.resource:
            return

        if self.release_at:
            asyncio.create_task(
                self._delayed_release(self.resource),
                name=f"delayed-release({self.resource.name})",
            )
        else:
            await self.manager.release(self.resource)

        self.resource = None

    def release_later(self, when: float) -> None:
        self.release_at = when

    def update_state(self, state: dict[str, object]) -> None:
        if self.resource:
            self.resource.state = state

    async def _delayed_release(self, resource: ResourceData) -> None:
        if self.release_at:
            await asyncio.sleep(self.release_at - time.time())
        await self.manager.release(resource)

    async def _apply_rate_limit(self, rate_limit_items: list[RateLimitItem]) -> None:
        if not self.rate_limiter or not self.resource:
            return

        release_ats: list[int] = []
        for rate_limit_item in rate_limit_items:
            reset_time, remaining = await self.rate_limiter.get_window_stats(
                rate_limit_item, self.resource.type, self.resource.name
            )
            await self.rate_limiter.hit(
                rate_limit_item, self.resource.type, self.resource.name
            )
            # using remaining below 1 because we are actually
            # consuming the last request available
            if remaining <= 1:
                release_ats.append(reset_time)

        if release_ats:
            rate_limit_release_at = max(release_ats)
            if not self.release_at or rate_limit_release_at > self.release_at:
                self.release_at = rate_limit_release_at

    async def __aenter__(self) -> "ResourceContext":
        if self.resource is None:
            raise ValueError("Cannot enter a released context")
        if self.resource.default_delay:
            self.release_at = time.time() + self.resource.default_delay
        if self.rate_limiter and self.resource.rate_limit:
            await self._apply_rate_limit(self.resource.rate_limit.rate_limit_items)

        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.release()


class ResourcesContext:
    def __init__(
        self,
        resources: dict[str, ResourceContext],
        exit_stack: contextlib.AsyncExitStack,
    ) -> None:
        self.resources = resources
        self.exit_stack = exit_stack

    async def release(self) -> None:
        # Add the resource back to the manager.
        await self.exit_stack.aclose()

    async def __aenter__(self) -> dict[str, ResourceContext]:
        return self.resources

    async def __aexit__(self, *exc: object) -> None:
        await self.release()


class ExclusiveResources:
    def __init__(self, limiters_storage: storage.Storage) -> None:
        self.availables: set[ResourceData] = set()
        self.used: set[ResourceData] = set()
        self.condition = asyncio.Condition(asyncio.Lock())
        self.resources: dict[str, ResourceData] = {}
        self.limiters_storage: storage.Storage = limiters_storage
        self.limiters: dict[str, RateLimiter] = {}

    async def acquire(self, *, wait: bool = True) -> ResourceContext:
        async with self.condition:
            if not wait:
                resource = self.try_acquire()
                if not resource:
                    raise ResourceUnavailable()
                return ResourceContext(resource, self)
            resource = await self.condition.wait_for(self.try_acquire)
            # wait_for above always return a non-none value. Assert is there
            # to make mypy happy.
            assert resource is not None  # noqa: S101
            return ResourceContext(resource, self)

    def _build_rate_limiter(
        self, resource_name: str, resource_rate_limit: ResourceRateLimit
    ) -> None:
        rate_limiter_class = STRATEGIES.get(resource_rate_limit.strategy)
        if not rate_limiter_class:
            raise ValueError(
                "Cannot add resource, "
                f"invalid rate limit strategy: {resource_rate_limit.strategy}"
            )

        self.limiters[resource_name] = rate_limiter_class(
            self.limiters_storage
        )  # type: ignore[abstract]

    async def add(self, resource: ResourceData) -> None:
        if resource.name in self.resources:
            raise ValueError("Cannot add a resource twice")

        async with self.condition:
            self.availables.add(resource)
            self.resources[resource.name] = resource
            if resource.rate_limit:
                self._build_rate_limiter(resource.name, resource.rate_limit)

            self.condition.notify()

    async def remove(self, resource_name: str) -> None:
        resource = self.resources.pop(resource_name, None)
        if resource:
            self.availables.discard(resource)
            self.used.discard(resource)
            self.limiters.pop(resource.name, None)

    def try_acquire(self) -> t.Optional[ResourceData]:
        if self.availables:
            resource = self.availables.pop()
            self.used.add(resource)
            return resource
        return None

    async def release(self, resource: ResourceData) -> None:
        if resource in self.used:
            async with self.condition:
                self.used.remove(resource)
                self.availables.add(resource)
                self.condition.notify()


class ResourcesManager:
    name = "resources_manager"

    def __init__(self) -> None:
        self.limiters_storage: storage.Storage = storage.MemoryStorage()
        self.resources: dict[str, ExclusiveResources] = defaultdict(
            lambda: ExclusiveResources(self.limiters_storage)
        )

    async def acquire(self, resource_type: str, wait: bool = True) -> ResourceContext:
        return await self.resources[resource_type].acquire(wait=wait)

    async def acquire_many(
        self, resource_types: Iterable[str], wait: bool = True
    ) -> ResourcesContext:
        # Ensure we lock in sorted order to avoid deadlock.
        sorted_types = sorted(resource_types)
        resources = {}
        # Use an ExitStack to ensure that any error while acquiring a resource
        # will release previously locked resource.
        async with contextlib.AsyncExitStack() as stack:
            for resource_type in sorted_types:
                resource = await self.acquire(resource_type, wait=wait)
                resources[resource_type] = await stack.enter_async_context(resource)
            # Disown the exit stack from this context. ResourcesContext is
            # going to be the new owner.
            stack = stack.pop_all()
        return ResourcesContext(resources, stack)

    async def release(self, resource: ResourceData) -> None:
        await self.resources[resource.type].release(resource)

    async def add(self, resource: ResourceData) -> None:
        await self.resources[resource.type].add(resource)

    async def remove(self, resource: ResourceKey) -> None:
        await self.resources[resource.type].remove(resource.name)
