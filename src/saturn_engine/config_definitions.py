from enum import Enum


class Env(Enum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


class WorkerConfig:
    job_store_cls: str
    executor_cls: str
    worker_manager_url: str


class RabbitMQConfig:
    url: str


class RayConfig:
    local: bool
    address: str


class SaturnConfig:
    env: Env
    worker: WorkerConfig
    rabbitmq: RabbitMQConfig
    ray: RayConfig
