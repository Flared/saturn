from typing import Type

from itertools import chain

from saturn_engine.utils import inspect as extra_inspect

from ..resources.manager import ResourcesManager
from . import BaseServices
from . import Config
from . import Hooks
from . import Service
from . import Services
from . import ServicesNamespace
from . import TService


class MissingServiceError(Exception):
    pass


class ServicesManager:
    def __init__(self, config: Config) -> None:
        config = config.register_interface(Hooks.name, Hooks.Options())
        self.services: Services = ServicesNamespace(
            config=config,
            hooks=Hooks.from_options(config.r.get("hooks", {})),
            resources_manager=ResourcesManager(),
        )
        self.loaded_services: list[Service] = []
        self.is_opened = False

        # Load optional services based on config.
        defered_services = []
        for service_cls_path in chain(
            config.c.services_manager.base_services, config.c.services_manager.services
        ):
            service_cls = extra_inspect.import_name(service_cls_path)
            try:
                self._load_service(service_cls)
            except MissingServiceError:
                defered_services.append(service_cls)

        defered_again = []
        # Try to load services as long as we are making progress..
        while defered_services:
            for service_cls in defered_services:
                try:
                    self._load_service(service_cls)
                except MissingServiceError:
                    defered_again.append(service_cls)

            if defered_again == defered_services:
                raise ValueError(f"Can't load services: {defered_services}")
            defered_services = defered_again

    async def open(self) -> None:
        if self.is_opened:
            return
        for service in self.loaded_services:
            await service.open()
        self.is_opened = True

    async def close(self) -> None:
        if not self.is_opened:
            return
        for service in reversed(self.loaded_services):
            await service.close()

    def _load_service(self, service_cls: Type[TService]) -> TService:
        if service_cls.name in self.services:
            raise ValueError(f"Cannot load '{service_cls.name}' twice")

        if service_cls.Options:
            try:
                self.services["config"] = self.services.s.config.register_interface(
                    service_cls.name, service_cls.Options
                )
            except Exception as e:
                raise ValueError(
                    f"Invalid service '{service_cls.name}' configuration"
                ) from e

        try:
            services = self.services.cast(service_cls.Services or BaseServices)
        except Exception as e:
            raise MissingServiceError() from e

        try:
            service = service_cls(services)
        except Exception as e:
            raise ValueError(f"Failed to load service '{service_cls.name}'") from e

        self.loaded_services.append(service)
        self.services[service_cls.name] = service
        return service

    # Useful for tests loading mock service.
    async def _reload_service(self, service_cls: Type[TService]) -> TService:
        if old_service := self.services.pop(service_cls.name, None):
            self.loaded_services.remove(old_service)
            await old_service.close()
        service = self._load_service(service_cls)
        await service.open()
        return service

    def has_loaded(self, service_cls: Type[TService]) -> bool:
        return service_cls.name in self.services
