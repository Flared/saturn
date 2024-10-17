from saturn_engine.config import Env
from saturn_engine.config import SaturnConfig
from saturn_engine.config import ServicesManagerConfig
from saturn_engine.config_definitions import WorkerManagerConfig


class config(SaturnConfig):
    env = Env.TEST

    class services_manager(ServicesManagerConfig):
        services: list[str] = [
            "saturn_engine.worker.services.tracing.Tracer",
            "saturn_engine.worker.services.metrics.Metrics",
            "saturn_engine.worker.services.usage_metrics.UsageMetrics",
            "saturn_engine.worker.services.loggers.Logger",
        ]

    class job_state:
        auto_flush = False

    class worker_manager(WorkerManagerConfig):
        static_definitions_directories = []
