class ServicesManager:
    def __init__(self) -> None:
        from .config import ConfigService
        from .rabbitmq import RabbitMQService

        self.rabbitmq = RabbitMQService
        self.config = ConfigService
