from saturn_engine.config import Env
from saturn_engine.config import SaturnConfig
from saturn_engine.config import ServicesManagerConfig
from saturn_engine.config import WorkerConfig


class config(SaturnConfig):
    env = Env.TEST

    class services_manager(ServicesManagerConfig):
        strict_services = False
        services: list[str] = [
            "saturn_engine.worker.services.tracing.Tracer",
            "saturn_engine.worker.services.metrics.Metrics",
            "saturn_engine.worker.services.usage_metrics.UsageMetrics",
            "saturn_engine.worker.services.loggers.Logger",
        ]

    class worker(WorkerConfig):
        job_store_cls = "MemoryJobStore"
