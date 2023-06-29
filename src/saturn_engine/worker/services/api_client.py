import typing as t
from saturn_engine.client.worker_manager import WorkerManagerClient

import dataclasses
from . import Service, BaseServices
from .http_client import HttpClient

@dataclasses.dataclass
class Options:
    api_url: str
    worker_id: t.Optional[str] = None

class Services(BaseServices):
    http_client: HttpClient

class ApiClientService(Service[Services, Options]):
    name = "api_client"

    Services = Services
    Options = Options

    client: WorkerManagerClient

    async def open(self) -> None:
        self.client = WorkerManagerClient(
            http_client=self.services.http_client.session,
            base_url=self.options.api_url,
            worker_id=self.options.worker_id,
        )
