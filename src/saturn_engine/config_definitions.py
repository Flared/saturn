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
    # Base services (override at your own risk...)
    base_services: list[str]
    # Extra services to load
    services: list[str]


@dataclasses.dataclass
class SaturnConfig:
    env: Env
    # Worker Manager URL used by clients and workers.
    worker_id: str
    worker_manager_url: str
    # If true, a Saturn Worker will tweak the config to run without a
    # Worker Manager
    standalone: bool
    # If set, select jobs matching the selector regex.
    selector: t.Optional[str]
    services_manager: ServicesManagerConfig
    worker_manager: WorkerManagerConfig
    rabbitmq: RabbitMQConfig
    redis: RedisConfig
