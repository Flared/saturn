import asyncio
import contextlib
from collections import defaultdict
from typing import Optional

from saturn_engine.core import Resource


class ResourceUnavailable(Exception):
    pass


class ResourceContext:
    def __init__(self, resource: Resource, manager: "ExclusiveResources") -> None:
        self.resource: Optional[Resource] = resource
        self.manager = manager

    async def release(self) -> None:
        # Add the resource back to the manager.
        if self.resource:
            await self.manager.add(self.resource)
        self.resource = None

    async def __aenter__(self) -> Resource:
        if self.resource is None:
            raise ValueError("Cannot enter a released context")
        return self.resource

    async def __aexit__(self, *exc: object) -> None:
        await self.release()


class ResourcesContext:
    def __init__(
        self, resources: dict[str, Resource], exit_stack: contextlib.AsyncExitStack
    ) -> None:
        self.resources = resources
        self.exit_stack = exit_stack

    async def release(self) -> None:
        # Add the resource back to the manager.
        await self.exit_stack.aclose()

    async def __aenter__(self) -> dict[str, Resource]:
        return self.resources

    async def __aexit__(self, *exc: object) -> None:
        await self.release()


class ExclusiveResources:
    def __init__(self) -> None:
        self.resources: set[Resource] = set()
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

    async def add(self, resource: Resource) -> None:
        if resource in self.resources:
            raise ValueError("Cannot add a resource twice")

        async with self.condition:
            self.resources.add(resource)
            self.condition.notify()

    def try_acquire(self) -> Optional[Resource]:
        if self.resources:
            return self.resources.pop()
        return None


class ResourcesManager:
    def __init__(self) -> None:
        self.resources: dict[str, ExclusiveResources] = defaultdict(ExclusiveResources)

    async def acquire(self, resource_type: str, wait: bool = True) -> ResourceContext:
        return await self.resources[resource_type].acquire(wait=wait)

    async def acquire_many(
        self, resource_types: list[str], wait: bool = True
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

    async def add(self, resource_type: str, resource: Resource) -> None:
        await self.resources[resource_type].add(resource)
