import typing as t

import dataclasses
from enum import Enum


class Env(Enum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


@dataclasses.dataclass
class WorkerManagerConfig:
    flask_host: str
    flask_port: int
    database_url: str
    database_connection_creator: t.Optional[str]
    database_pool_recycle: int
    database_pool_pre_ping: bool
    static_definitions_directories: list[str]
    static_definitions_jobs_selector: t.Optional[str]
    work_items_per_worker: int


@dataclasses.dataclass
class RabbitMQConfig:
    url: str
    urls: dict[str, str]
    reconnect_interval: int


@dataclasses.dataclass
class RedisConfig:
    dsn: str


@dataclasses.dataclass
class ServicesManagerConfig:
    # Services to load
    services: list[str]
    # Check services type dependancies match loaded services. `False` value is
    # needed to load fake services.
    strict_services: bool


@dataclasses.dataclass
class SaturnConfig:
    env: Env
    # Worker Manager URL used by clients and workers.
    worker_id: str
    worker_manager_url: str
    services_manager: ServicesManagerConfig
    worker_manager: WorkerManagerConfig
    rabbitmq: RabbitMQConfig
    redis: RedisConfig
