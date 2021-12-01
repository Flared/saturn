import os
from enum import Enum
from typing import Any
from typing import Type


class Env(Enum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


class BaseConfig:
    env = "development"

    class worker:
        job_store_cls = "ApiJobStore"

    class rabbitmq:
        url = os.environ.get("SATURN_AMQP_URL", "amqp://127.0.0.1/")

    class worker_manager:
        url = os.environ.get("SATURN_WORKER_MANAGER_URL", "http://localhost:5000")


class TestConfig(BaseConfig):
    class worker(BaseConfig.worker):
        job_store_cls = "MemoryJobStore"


class ConfigService:
    _config: Type[BaseConfig]

    def __init__(self) -> None:
        env = Env(os.environ["SATURN_ENV"])
        if env is Env.TEST:
            self._config = TestConfig
        else:
            self._config = BaseConfig

    def __getattr__(self, name: str) -> Any:
        return getattr(self._config, name)
