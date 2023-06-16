from typing import Type

from saturn_engine.utils import inspect as extra_inspect

from ..resources.manager import ResourcesManager
from . import BaseServices
from . import Config
from . import Hooks
from . import Service
from . import Services
from . import ServicesNamespace
from . import TService


class ServicesManager:
    def __init__(self, config: Config) -> None:
        self.strict = config.c.services_manager.strict_services
        self.services: Services = ServicesNamespace(
            config=config,
            hooks=Hooks(),
            resources_manager=ResourcesManager(),
            strict=self.strict,
        )
        self.loaded_services: list[Service] = []
        self.is_opened = False

        # Some services are required for saturn to work at all.
        for service_cls in BASE_SERVICES:
            self._load_service(service_cls)

        # Load optional services based on config.
        for service_cls_path in config.c.services_manager.services:
            service_cls = extra_inspect.import_name(service_cls_path)
            self._load_service(service_cls)

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
            await service._shutdown()

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
            service = service_cls(
                self.services.cast(service_cls.Services or BaseServices)
            )
        except Exception as e:
            raise ValueError(f"Failed to load service '{service_cls.name}'") from e
        self.loaded_services.append(service)
        self.services[service_cls.name] = service
        return service

    # Useful for tests loading mock service.
    async def _reload_service(self, service_cls: Type[TService]) -> TService:
        if old_service := self.services.pop(service_cls.name, None):
            self.loaded_services.remove(old_service)
            await old_service._shutdown()
        return self._load_service(service_cls)

    def has_loaded(self, service_cls: Type[TService]) -> bool:
        return service_cls.name in self.services


from .http_client import HttpClient
from .job_store import JobStoreService
from .tasks_runner import TasksRunnerService

BASE_SERVICES: list[Type[Service]] = [
    HttpClient,
    JobStoreService,
    TasksRunnerService,
]
