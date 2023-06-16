import typing as t
from typing import Any
from typing import ClassVar
from typing import Generic
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import cast

import asyncio
from collections.abc import Coroutine
from functools import cached_property

from saturn_engine.config import Config
from saturn_engine.utils import Namespace
from saturn_engine.utils import inspect as extra_inspect
from saturn_engine.utils.asyncutils import TasksGroupRunner
from saturn_engine.utils.config import Config as BaseConfig
from saturn_engine.utils.config import LazyConfig
from saturn_engine.worker.resources.manager import ResourcesManager

from .hooks import Hooks

__all__ = ("Config", "Service", "BaseServices")


class BaseServices:
    config: Config
    resources_manager: ResourcesManager
    hooks: Hooks


TServices = TypeVar("TServices", bound=BaseServices)
TOptions = TypeVar("TOptions")


class ConfigContainer(t.Protocol):
    @property
    def config(self) -> t.Union[LazyConfig, BaseConfig]:
        ...


T = TypeVar("T")
U = TypeVar("U")


class Service(Generic[TServices, TOptions]):
    name: ClassVar[str]
    Services: Optional[Type[TServices]] = None
    Options: Optional[Type[TOptions]] = None

    @t.final
    def __init__(self, services: TServices):
        self.services: TServices = services
        self.__task_runner: t.Optional[asyncio.Task] = None

    @property
    def options(self) -> TOptions:
        return self.options_from(self.services)

    def options_from(self, obj: ConfigContainer) -> TOptions:
        if self.Options is None:
            raise ValueError("No options defined.")
        return obj.config.cast_namespace(self.name, self.Options)

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None

    @cached_property
    def tasks(self) -> TasksGroupRunner:
        tasks = TasksGroupRunner(name=f"service-{self.name}")
        self.__task_runner = asyncio.create_task(
            self.tasks.run(), name=f"service-{self.name}.runner"
        )
        return tasks

    async def _stop_tasks(self) -> None:
        if "tasks" in self.__dict__:
            await self.tasks.close()
            del self.tasks
        if self.__task_runner:
            await self.__task_runner

    def create_task(self, coroutine: Coroutine, **kwargs: t.Any) -> asyncio.Task:
        return self.tasks.create_task(coroutine, **kwargs)

    @t.final
    async def _shutdown(self) -> None:
        await self.close()
        await self._stop_tasks()


TService = TypeVar("TService", bound=Service)

MinimalService = Service[BaseServices, None]


class ServicesNamespace(Namespace, Generic[T]):
    def __init__(self, *args: Any, strict: bool, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.strict = strict
        self.s: T = cast(T, self)

    def cast(self, interface: Type[U]) -> "ServicesNamespace[U]":
        services_annotations = t.get_type_hints(interface)
        for service in services_annotations.values():
            self.cast_service(service)
        return cast(ServicesNamespace[U], self)

    def cast_service(self, service_cls: Type[TService]) -> TService:
        # Config is a special case (Doesn't subclass Service).
        if issubclass(service_cls, BaseConfig):
            return self["config"]

        name = service_cls.name
        typ_import_name = extra_inspect.get_import_name(service_cls)
        service = self.get(name)
        if not service:
            raise ValueError(f"Namespace missing '{typ_import_name}' service")

        if self.strict and service_cls is not service.__class__:
            dependency_name = extra_inspect.get_import_name(service.__class__)
            raise ValueError(
                f"Service '{name}' expected to be '{typ_import_name}', "
                f"got '{dependency_name}'"
            )

        return cast(TService, service)


Services = ServicesNamespace[BaseServices]
