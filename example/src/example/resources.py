import typing as t

import dataclasses
from collections.abc import Collection

from saturn_engine.core import Resource
from saturn_engine.worker.resources.provider import PeriodicSyncOptions
from saturn_engine.worker.resources.provider import PeriodicSyncProvider
from saturn_engine.worker.resources.provider import ProvidedResource


@dataclasses.dataclass
class TestApiKey(Resource):
    key: str
    state: t.Optional[dict[str, int]] = None


@dataclasses.dataclass
class BackpressureApiKey(Resource):
    pass


@dataclasses.dataclass
class ProviderApiKey(TestApiKey):
    pass


class ApiKeysProvider(PeriodicSyncProvider["ApiKeysProvider.Options"]):
    @dataclasses.dataclass
    class Options(PeriodicSyncOptions):
        key: str

    n: int

    async def open(self) -> None:
        self.n = 0

    async def sync(self) -> Collection[ProvidedResource]:
        # Provide a new key every 5 calls
        resource = ProvidedResource(
            name=f"provider-api-key-{self.n}",
            data={"key": self.options.key + str(self.n)},
        )
        self.n += 1
        return [resource]
