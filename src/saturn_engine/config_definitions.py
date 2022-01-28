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
class WorkerManagerConfig:
    flask_host: str
    flask_port: int
    database_url: str
    async_database_url: str
    static_definitions_directory: str
    work_items_per_worker: int


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
    worker_manager: WorkerManagerConfig
    rabbitmq: RabbitMQConfig
    ray: RayConfig
