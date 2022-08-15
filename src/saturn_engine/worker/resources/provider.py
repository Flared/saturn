import typing as t

import abc
import dataclasses

from saturn_engine.core.api import ResourcesProviderItem
from saturn_engine.utils.options import OptionsSchema
from saturn_engine.worker.resources.manager import ResourceData
from saturn_engine.worker.resources.manager import ResourceKey
from saturn_engine.worker.services import Services


@dataclasses.dataclass
class ProvidedResource:
    name: str
    data: dict[str, object]
    default_delay: float = 0


TOptions = t.TypeVar("TOptions")


class ResourcesProvider(abc.ABC, OptionsSchema, t.Generic[TOptions]):
    options: TOptions

    def __init__(
        self,
        *,
        options: TOptions,
        services: Services,
        definition: ResourcesProviderItem,
    ) -> None:
        self.options = options
        self.services = services
        self.definition = definition
        self.managed_resources: set[ResourceKey] = set()

    @abc.abstractmethod
    async def open(self) -> None:
        pass

    async def close(self) -> None:
        for resource in list(self.managed_resources):
            await self.remove(resource)

    async def add(self, item: ProvidedResource) -> None:
        resource = ResourceData(
            name=item.name,
            type=self.definition.resource_type,
            data=item.data,
            default_delay=item.default_delay,
        )
        self.managed_resources.add(resource.key)
        await self.services.s.resources_manager.add(resource)

    async def remove(self, resource_key: ResourceKey) -> None:
        self.managed_resources.discard(resource_key)
        await self.services.s.resources_manager.remove(resource_key)


class StaticResourcesProvider(ResourcesProvider["StaticResourcesProvider.Options"]):
    @dataclasses.dataclass
    class Options:
        resources: list[ProvidedResource]

    async def open(self) -> None:
        for resource in self.options.resources:
            await self.add(resource)
