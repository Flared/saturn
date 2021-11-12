from typing import cast


class ServicesManager:
    def __init__(self) -> None:
        from .config import BaseConfig
        from .config import ConfigService
        from .http_client import HttpClient
        from .job_store import JobStoreService
        from .rabbitmq import RabbitMQService

        self.config = cast(BaseConfig, ConfigService())
        self.rabbitmq = RabbitMQService(self)
        self.http_client = HttpClient()
        self.job_store = JobStoreService(self)

    async def close(self) -> None:
        await self.rabbitmq.close()
        await self.http_client.close()
