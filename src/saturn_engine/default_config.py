import typing as t

import os

from .config import Env
from .config import RabbitMQConfig
from .config import RedisConfig
from .config import SaturnConfig
from .config import ServicesManagerConfig
from .config import WorkerConfig
from .config import WorkerManagerConfig


class config(SaturnConfig):
    env = Env(os.environ.get("SATURN_ENV", "development"))
    worker_manager_url = os.environ.get(
        "SATURN_WORKER_MANAGER_URL", "http://127.0.0.1:5000"
    )

    class services_manager(ServicesManagerConfig):
        services = [
            "saturn_engine.worker.services.labels_propagator.LabelsPropagator",
            "saturn_engine.worker.services.tracing.Tracer",
            "saturn_engine.worker.services.metrics.Metrics",
            "saturn_engine.worker.services.loggers.Logger",
            "saturn_engine.worker.services.rabbitmq.RabbitMQService",
        ]
        strict_services = True

    class worker(WorkerConfig):
        job_store_cls = "ApiJobStore"

    class rabbitmq(RabbitMQConfig):
        url = os.environ.get("SATURN_AMQP_URL", "amqp://127.0.0.1/")
        reconnect_interval = 10

    class worker_manager(WorkerManagerConfig):
        flask_host = os.environ.get("SATURN_FLASK_HOST", "127.0.0.1")
        flask_port = int(os.environ.get("SATURN_FLASK_PORT", 5000))
        database_url: str = os.environ.get("SATURN_DATABASE_URL", "sqlite:///test.db")
        database_connection_creator: t.Optional[str] = None
        database_pool_recycle: int = -1
        database_pool_pre_ping: bool = False
        static_definitions_directories: list[str] = os.environ.get(
            "SATURN_STATIC_DEFINITIONS_DIRS", "/opt/saturn/definitions"
        ).split(":")
        static_definitions_jobs_selector: t.Optional[str] = os.environ.get(
            "SATURN_STATIC_DEFINITIONS_JOBS_SELECTOR"
        )
        work_items_per_worker = 10

    class redis(RedisConfig):
        dsn = "redis://localhost:6379"

    class tracer:
        rate: float = 0.0


class client_config(config):
    class services_manager(config.services_manager):
        services = [
            "saturn_engine.worker.services.rabbitmq.RabbitMQService",
        ]
