import os

from .config import Env
from .config import RabbitMQConfig
from .config import RayConfig
from .config import SaturnConfig
from .config import WorkerConfig


class config(SaturnConfig):
    env = Env(os.environ.get("SATURN_ENV", "development"))

    class worker(WorkerConfig):
        job_store_cls = "ApiJobStore"
        executor_cls = os.environ.get("SATURN_WORKER__EXECUTOR_CLS", "ProcessExecutor")
        worker_manager_url = os.environ.get(
            "SATURN_WORKER_MANAGER_URL", "http://localhost:5000"
        )

    class rabbitmq(RabbitMQConfig):
        url = os.environ.get("SATURN_AMQP_URL", "amqp://127.0.0.1/")

    class ray(RayConfig):
        local = os.environ.get("SATURN_RAY__LOCAL", "0") == "1"
        address = os.environ.get("SATURN_RAY__ADDRESS", "auto")
