import typing as t

import abc
import asyncio
import dataclasses
from collections.abc import Collection

from saturn_engine.core.api import ResourcesProviderItem
from saturn_engine.utils.log import getLogger
from saturn_engine.utils.options import OptionsSchema
from saturn_engine.worker.resources.manager import ResourceData
from saturn_engine.worker.resources.manager import ResourceKey
from saturn_engine.worker.resources.manager import ResourceRateLimit
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.tasks_runner import TasksRunnerService


@dataclasses.dataclass
class ProvidedResourceRateLimit:
    rate_limits: list[str]
    strategy: str = "fixed-window"


@dataclasses.dataclass
class ProvidedResource:
    name: str
    data: dict[str, object]
    default_delay: float = 0
    rate_limit: t.Optional[ProvidedResourceRateLimit] = None


TOptions = t.TypeVar("TOptions")


@dataclasses.dataclass
class PeriodicSyncOptions:
    sync_interval: int


TPeriodicSyncOptions = t.TypeVar("TPeriodicSyncOptions", bound=PeriodicSyncOptions)


class ResourcesProvider(abc.ABC, OptionsSchema, t.Generic[TOptions]):
    options: TOptions

    def __init__(
        self,
        *,
        options: TOptions,
        services: Services,
        definition: ResourcesProviderItem,
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.options = options
        self.services = services
        self.definition = definition
        self._managed_resources: set[str] = set()

    @abc.abstractmethod
    async def open(self) -> None:
        pass

    async def _open(self) -> None:
        await self.open()

    async def close(self) -> None:
        pass

    async def _close(self) -> None:
        await self.close()
        for resource in list(self._managed_resources):
            await self.remove(resource)

    async def add(self, item: ProvidedResource) -> None:
        if item.name in self._managed_resources:
            return

        resource = ResourceData(
            name=item.name,
            type=self.definition.resource_type,
            data=item.data,
            default_delay=item.default_delay,
            rate_limit=ResourceRateLimit(
                rate_limits=item.rate_limit.rate_limits,
                strategy=item.rate_limit.strategy,
            )
            if item.rate_limit
            else None,
        )

        self._managed_resources.add(resource.name)
        await self.services.s.resources_manager.add(resource)

    async def remove(self, resource_name: str) -> None:
        self._managed_resources.discard(resource_name)
        await self.services.s.resources_manager.remove(
            ResourceKey(type=self.definition.resource_type, name=resource_name)
        )


class PeriodicSyncProvider(ResourcesProvider[TPeriodicSyncOptions]):
    async def open(self) -> None:
        pass

    async def _open(self) -> None:
        await super()._open()
        tasks_runner = self.services.cast_service(TasksRunnerService)
        self._sync_task = tasks_runner.create_task(
            self.poller(), name=f"provider-sync({self.definition.name})"
        )

    async def close(self) -> None:
        self._sync_task.cancel()

    async def poller(self) -> None:
        while True:
            try:
                resources = await self.sync()
                await self.update(resources)
            except Exception:
                self.logger.exception("Failed to sync resources")
            await asyncio.sleep(self.options.sync_interval)

    async def update(self, resources: Collection[ProvidedResource]) -> None:
        # To add
        for resource in resources:
            await self.add(resource)
        # To remove
        to_remove = self._managed_resources - {r.name for r in resources}
        for resource_name in to_remove:
            await self.remove(resource_name)

    @abc.abstractmethod
    async def sync(self) -> Collection[ProvidedResource]:
        return []


class StaticResourcesProvider(ResourcesProvider["StaticResourcesProvider.Options"]):
    @dataclasses.dataclass
    class Options:
        resources: list[ProvidedResource]

    async def open(self) -> None:
        for resource in self.options.resources:
            await self.add(resource)


BUILTINS: dict[str, t.Type[ResourcesProvider]] = {
    "StaticResourcesProvider": StaticResourcesProvider,
}
