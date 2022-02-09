import os

from saturn_engine.config import SaturnConfig
from saturn_engine.config import WorkerConfig
from saturn_engine.default_config import config as default_config
from saturn_engine.worker.services.extras.sentry import Sentry


class config(SaturnConfig):
    class worker(WorkerConfig):
        services = default_config.services_manager.services + [
            "saturn_engine.worker.services.extras.sentry.Sentry",
        ]

    class sentry(Sentry.Options):
        dsn = os.environ.get("SATURN_SENTRY")
