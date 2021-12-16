import aio_pika
import asyncstdlib as alib

from saturn_engine.utils import get_own_attr

from . import MinimalService


class RabbitMQService(MinimalService):
    name = "rabbitmq"

    @alib.cached_property
    async def connection(self) -> aio_pika.Connection:
        return await aio_pika.connect_robust(self.services.config.c.rabbitmq.url)

    async def close(self) -> None:
        connection = get_own_attr(self, "connection", None)
        if connection is not None:
            await (await connection).close()
