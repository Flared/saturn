class ServicesManager:
    def __init__(self) -> None:
        from .config import ConfigService
        from .http_client import HttpClient
        from .rabbitmq import RabbitMQService

        self.rabbitmq = RabbitMQService(self)
        self.config = ConfigService()
        self.http_client = HttpClient()

    async def close(self) -> None:
        await self.rabbitmq.close()
        await self.http_client.close()
