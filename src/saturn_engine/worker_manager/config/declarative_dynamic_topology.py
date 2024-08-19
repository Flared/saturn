import typing as t

import dataclasses

from saturn_engine.utils import inspect as extra_inspect
from saturn_engine.utils.declarative_config import BaseObject
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions

DYNAMIC_TOPOLOGY_KIND: t.Final[str] = "SaturnDynamicTopology"


class DynamicTopologyModule(t.Protocol):
    def __call__(self, definitions: StaticDefinitions) -> None: ...


@dataclasses.dataclass
class DynamicTopologySpec:
    module: str


@dataclasses.dataclass(kw_only=True)
class DynamicTopology(BaseObject):
    spec: DynamicTopologySpec
    kind: str = DYNAMIC_TOPOLOGY_KIND

    def update_static_definitions(self, definitions: StaticDefinitions) -> None:
        t.cast(
            DynamicTopologyModule,
            extra_inspect.import_name(self.spec.module),
        )(definitions)
