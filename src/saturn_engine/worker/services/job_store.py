from saturn_engine.core.api import QueueItem
from saturn_engine.worker.job import JobStore
from saturn_engine.worker.job.api import ApiJobStore
from saturn_engine.worker.job.memory import MemoryJobStore

from .manager import ServicesManager


class JobStoreService:
    def __init__(self, services: ServicesManager) -> None:
        self.services = services

    def for_queue(self, queue: QueueItem) -> JobStore:
        klass = self.services.config.c.worker.job_store_cls
        if klass == "MemoryJobStore":
            return MemoryJobStore()
        if klass == "ApiJobStore":
            return ApiJobStore(
                http_client=self.services.http_client.session,
                base_url=self.services.config.c.worker.worker_manager_url,
                job_name=queue.name,
            )
        raise ValueError(f"Unkown job store class: {klass}")
