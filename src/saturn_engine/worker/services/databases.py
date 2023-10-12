import typing as t

import dataclasses

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Session

from saturn_engine.utils.log import getLogger

from . import BaseServices
from . import Service


@dataclasses.dataclass
class Options:
    engines: dict = dataclasses.field(default_factory=dict)
    sync_engines: dict = dataclasses.field(default_factory=dict)


class Databases(Service[BaseServices, Options]):
    engines: dict[str, AsyncEngine]
    sync_engines: dict[str, Engine]

    name = "databases"
    DEFAULT_NAME = "default"

    Services = BaseServices
    Options = Options

    async def open(self) -> None:
        self.logger = getLogger(__name__, self)
        self.engines = {}
        self.sync_engines = {}

    def engine(self, name: str = "default") -> AsyncEngine:
        if engine := self.engines.get(name):
            return engine

        url, params = self._get_engine_params(name, sync=False)
        engine = create_async_engine(url, **params)
        self.engines[name] = engine
        return engine

    def session(self, engine_name: str = "default", **kwargs: t.Any) -> AsyncSession:
        return AsyncSession(self.engine(engine_name), **kwargs)

    def sync_engine(self, name: str = "default") -> Engine:
        if engine := self.sync_engines.get(name):
            return engine

        url, params = self._get_engine_params(name, sync=True)
        engine = create_engine(url, **params)
        if not engine:
            raise ValueError(f"Failed to create engine '{name}'")
        self.sync_engines[name] = engine
        return engine

    def sync_session(self, engine_name: str = "default", **kwargs: t.Any) -> Session:
        return Session(self.sync_engine(engine_name), **kwargs)

    def _get_engine_params(self, name: str, *, sync: bool) -> tuple[str, dict]:
        key = "engines"
        if sync:
            key = "sync_engines"

        url: str = ""
        params: dict[str, t.Any] = {}

        url_or_params = getattr(self.options, key)[name]
        if isinstance(url_or_params, str):
            url = url_or_params
        elif isinstance(url_or_params, tuple):
            url, params = url_or_params
        else:
            raise ValueError(f"Invalid config: {url_or_params}")
        return url, params
