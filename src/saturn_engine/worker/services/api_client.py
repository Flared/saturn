import asyncio

from sqlalchemy.orm import sessionmaker

from saturn_engine.client.worker_manager import WorkerManagerClient
from saturn_engine.worker.services.databases import Databases
from saturn_engine.worker.services.tasks_runner import TasksRunnerService
from saturn_engine.worker.worker_manager import StandaloneWorkerManagerClient

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
            executors=self.services.config.c.executors,
            worker_id=self.services.config.c.worker_id,
            selector=self.services.config.c.selector,
        )


class StandaloneServices(BaseServices):
    databases: Databases
    tasks_runner: TasksRunnerService


class StandaloneApiClient(Service[StandaloneServices, None]):
    name = "api_client"

    Services = StandaloneServices

    client: StandaloneWorkerManagerClient

    SYNC_DELAY = 60

    async def open(self) -> None:
        self.client = StandaloneWorkerManagerClient(
            config=self.services.config,
            sessionmaker=sessionmaker(self.services.databases.sync_engine()),
        )

        await self.client.init_db()
        await self.client.sync_jobs()
        self.services.tasks_runner.create_task(
            self._sync_jobs(), name="StandaloneClient.sync-jobs"
        )

    async def _sync_jobs(self) -> None:
        while True:
            await asyncio.sleep(self.SYNC_DELAY)
            await self.client.sync_jobs()
