import dataclasses

from .services.manager import ServicesManager


@dataclasses.dataclass
class Context:
    services: ServicesManager
