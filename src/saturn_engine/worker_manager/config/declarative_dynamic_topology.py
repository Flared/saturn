import typing as t

import dataclasses

from saturn_engine.utils import inspect as extra_inspect
from saturn_engine.utils.declarative_config import BaseObject
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions


class DynamicTopologyModule(t.Protocol):
    def __call__(self, definitions: StaticDefinitions) -> None: ...


@dataclasses.dataclass
class DynamicTopologySpec:
    module: str


@dataclasses.dataclass
class DynamicTopology(BaseObject):
    spec: DynamicTopologySpec

    def update_static_definitions(self, definitions: StaticDefinitions) -> None:
        t.cast(
            DynamicTopologyModule,
            extra_inspect.import_name(self.spec.module),
        )(definitions)
