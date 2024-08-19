import typing as t

import dataclasses

from saturn_engine.core import api
from saturn_engine.utils.declarative_config import BaseObject

RESOURCE_KIND: t.Final[str] = "SaturnResource"
RESOURCE_PROVIDER_KIND: t.Final[str] = "SaturnResourcesProvider"


@dataclasses.dataclass
class ResourceRateLimitSpec:
    rate_limits: list[str]
    strategy: str = "fixed-window"


@dataclasses.dataclass
class ResourceSpec:
    type: str
    data: dict[str, t.Any]
    default_delay: float = 0
    rate_limit: t.Optional[ResourceRateLimitSpec] = None
    concurrency: int = 1


@dataclasses.dataclass(kw_only=True)
class Resource(BaseObject):
    spec: ResourceSpec
    kind: str = RESOURCE_KIND

    def to_core_object(self) -> api.ResourceItem:
        return api.ResourceItem(
            name=self.metadata.name,
            type=self.spec.type,
            data=self.spec.data,
            default_delay=self.spec.default_delay,
            rate_limit=(
                api.ResourceRateLimitItem(
                    rate_limits=self.spec.rate_limit.rate_limits,
                    strategy=self.spec.rate_limit.strategy,
                )
                if self.spec.rate_limit
                else None
            ),
        )


@dataclasses.dataclass
class ResourcesProviderSpec:
    type: str
    resource_type: str
    options: dict[str, t.Any]


@dataclasses.dataclass(kw_only=True)
class ResourcesProvider(BaseObject):
    spec: ResourcesProviderSpec
    kind: str = RESOURCE_PROVIDER_KIND

    def to_core_object(self) -> api.ResourcesProviderItem:
        return api.ResourcesProviderItem(
            name=self.metadata.name,
            type=self.spec.type,
            resource_type=self.spec.resource_type,
            options=self.spec.options,
        )
