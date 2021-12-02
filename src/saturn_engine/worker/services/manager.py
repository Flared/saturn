from saturn_engine.config import Config


class ServicesManager:
    def __init__(self, config: Config) -> None:
        from .http_client import HttpClient
        from .job_store import JobStoreService
        from .rabbitmq import RabbitMQService

        self.config = config
        self.rabbitmq = RabbitMQService(self)
        self.http_client = HttpClient()
        self.job_store = JobStoreService(self)

    async def close(self) -> None:
        await self.rabbitmq.close()
        await self.http_client.close()
