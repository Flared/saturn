import aio_pika
import asyncstdlib as alib

from saturn_engine.utils import get_own_attr

from .manager import ServicesManager


class RabbitMQService:
    def __init__(self, services: ServicesManager) -> None:
        self.services = services

    @alib.cached_property
    async def connection(self) -> aio_pika.Connection:
        return await aio_pika.connect_robust(self.services.config.rabbitmq.url)

    async def close(self) -> None:
        connection = get_own_attr(self, "connection", None)
        if connection is not None:
            await (await connection).close()
