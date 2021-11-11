import asyncio
import contextlib
import dataclasses
import time
from collections import defaultdict
from collections.abc import Iterable
from typing import Optional


@dataclasses.dataclass(eq=False)
class ResourceData:
    name: str
    type: str
    data: dict[str, object]
    default_delay: float = 0


class ResourceUnavailable(Exception):
    pass


class ResourceContext:
    def __init__(self, resource: ResourceData, manager: "ExclusiveResources") -> None:
        self.resource: Optional[ResourceData] = resource
        self.manager = manager
        self.release_at: Optional[float] = None

    async def release(self) -> None:
        # Add the resource back to the manager.
        if not self.resource:
            return

        if self.release_at:
            asyncio.create_task(self._delayed_release(self.resource))
        else:
            await self.manager.release(self.resource)

        self.resource = None

    def release_later(self, when: float) -> None:
        self.release_at = when

    async def _delayed_release(self, resource: ResourceData) -> None:
        if self.release_at:
            await asyncio.sleep(self.release_at - time.time())
        await self.manager.release(resource)

    async def __aenter__(self) -> "ResourceContext":
        if self.resource is None:
            raise ValueError("Cannot enter a released context")
        if self.resource.default_delay:
            self.release_at = time.time() + self.resource.default_delay
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
    def __init__(self) -> None:
        self.availables: set[ResourceData] = set()
        self.used: set[ResourceData] = set()
        self.condition = asyncio.Condition(asyncio.Lock())

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

    async def add(self, resource: ResourceData) -> None:
        if resource in self.availables or resource in self.used:
            raise ValueError("Cannot add a resource twice")

        async with self.condition:
            self.availables.add(resource)
            self.condition.notify()

    async def remove(self, resource: ResourceData) -> None:
        self.availables.discard(resource)
        self.used.discard(resource)

    def try_acquire(self) -> Optional[ResourceData]:
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
    def __init__(self) -> None:
        self.resources: dict[str, ExclusiveResources] = defaultdict(ExclusiveResources)

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

    def remove(self, resource: ResourceData) -> None:
        self.resources[resource.type].remove(resource)
