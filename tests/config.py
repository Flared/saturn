from saturn_engine.config import Env
from saturn_engine.config import RayConfig
from saturn_engine.config import SaturnConfig
from saturn_engine.config import ServicesManagerConfig
from saturn_engine.config import WorkerConfig


class config(SaturnConfig):
    env = Env.TEST

    class services_manager(ServicesManagerConfig):
        strict_services = False
        services: list[str] = []

    class worker(WorkerConfig):
        job_store_cls = "MemoryJobStore"

    class ray(RayConfig):
        local = True
        address = ""
