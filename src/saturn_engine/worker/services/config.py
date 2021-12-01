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
        executor_cls = os.environ.get("SATURN_WORKER__EXECUTOR_CLS", "ProcessExecutor")

    class rabbitmq:
        url = os.environ.get("SATURN_AMQP_URL", "amqp://127.0.0.1/")

    class worker_manager:
        url = os.environ.get("SATURN_WORKER_MANAGER_URL", "http://localhost:5000")

    class ray:
        local = os.environ.get("SATURN_RAY__LOCAL", "0") == "1"
        address = os.environ.get("SATURN_RAY__ADDRESS", "auto")


class TestConfig(BaseConfig):
    class worker(BaseConfig.worker):
        job_store_cls = "MemoryJobStore"

    class ray(BaseConfig.ray):
        local = True
        address = ""


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
