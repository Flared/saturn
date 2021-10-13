import dataclasses

from ..services.manager import ServicesManager


@dataclasses.dataclass
class QueueContext:
    services: ServicesManager
