import dataclasses
from enum import Enum


class Env(Enum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


@dataclasses.dataclass
class WorkerConfig:
    job_store_cls: str
    executor_cls: str
    worker_manager_url: str
    services: list[str]
    # Check services type dependancies match loaded services. `False` value is
    # needed to load fake services.
    strict_services: bool


@dataclasses.dataclass
class RabbitMQConfig:
    url: str


@dataclasses.dataclass
class RayConfig:
    local: bool
    address: str


@dataclasses.dataclass
class SaturnConfig:
    env: Env
    worker: WorkerConfig
    rabbitmq: RabbitMQConfig
    ray: RayConfig
