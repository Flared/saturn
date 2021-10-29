class ServicesManager:
    def __init__(self) -> None:
        from .config import ConfigService
        from .inventories import InventoriesService
        from .rabbitmq import RabbitMQService
        from .resources_manager import ResourcesManager

        self.rabbitmq = RabbitMQService(self)
        self.inventories = InventoriesService()
        self.config = ConfigService()
        self.resources_manager = ResourcesManager()

    async def close(self) -> None:
        await self.rabbitmq.close()
