import typing
from typing import Any
from typing import ClassVar
from typing import Generic
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import cast

from saturn_engine.config import Config
from saturn_engine.utils import Namespace
from saturn_engine.utils import inspect as extra_inspect
from saturn_engine.utils.config import Config as BaseConfig

from .hooks import Hooks

__all__ = ("Config", "Service", "BaseServices")


class BaseServices:
    config: Config
    hooks: Hooks


TServices = TypeVar("TServices", bound=BaseServices)
TOptions = TypeVar("TOptions")

T = TypeVar("T")
U = TypeVar("U")


class Service(Generic[TServices, TOptions]):
    name: ClassVar[str]
    Services: Optional[Type[TServices]] = None
    Options: Optional[Type[TOptions]] = None

    def __init__(self, services: TServices):
        self.services: TServices = services

    @property
    def options(self) -> TOptions:
        if self.Options is None:
            raise ValueError("No options defined.")
        return self.services.config.cast_namespace(self.name, self.Options)

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None


TService = TypeVar("TService", bound=Service)

MinimalService = Service[BaseServices, None]


class ServicesNamespace(Namespace, Generic[T]):
    def __init__(self, *args: Any, strict: bool, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.strict = strict
        self.s: T = cast(T, self)

    def cast(self, interface: Type[U]) -> "ServicesNamespace[U]":
        services_annotations = typing.get_type_hints(interface)
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
