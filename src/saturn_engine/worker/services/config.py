import dataclasses
import os
from functools import cached_property


@dataclasses.dataclass
class RabbitMQConfig:
    url: str


@dataclasses.dataclass
class WorkerManagerConfig:
    url: str


class ConfigService:
    def __init__(self) -> None:
        pass

    @cached_property
    def rabbitmq(self) -> RabbitMQConfig:
        return RabbitMQConfig(
            url=os.environ.get("SATURN_AMQP_URL", "amqp://127.0.0.1/"),
        )

    @cached_property
    def worker_manager(self) -> WorkerManagerConfig:
        return WorkerManagerConfig(
            url=os.environ.get("SATURN_WORKER_MANAGER_URL", "http://localhost:5000"),
        )
