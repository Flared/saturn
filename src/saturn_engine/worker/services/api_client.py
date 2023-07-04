from saturn_engine.client.worker_manager import WorkerManagerClient

from . import BaseServices
from . import Service
from .http_client import HttpClient


class Services(BaseServices):
    http_client: HttpClient


class ApiClient(Service[Services, None]):
    name = "api_client"

    Services = Services

    client: WorkerManagerClient

    async def open(self) -> None:
        self.client = WorkerManagerClient(
            http_client=self.services.http_client.session,
            base_url=self.services.config.c.worker_manager_url,
            worker_id=self.services.config.c.worker_id,
        )
