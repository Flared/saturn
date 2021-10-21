class ServicesManager:
    def __init__(self) -> None:
        from .config import ConfigService
        from .inventories import InventoriesService
        from .rabbitmq import RabbitMQService

        self.rabbitmq = RabbitMQService(self)
        self.inventories = InventoriesService()
        self.config = ConfigService()

    async def close(self) -> None:
        await self.rabbitmq.close()
