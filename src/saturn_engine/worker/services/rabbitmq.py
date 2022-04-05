import aio_pika
import aio_pika.abc

from saturn_engine.utils.asyncutils import cached_property

from . import MinimalService


class RabbitMQService(MinimalService):
    name = "rabbitmq"

    @cached_property
    async def connection(self) -> aio_pika.abc.AbstractRobustConnection:
        return await aio_pika.connect_robust(self.services.config.c.rabbitmq.url)

    async def close(self) -> None:
        if "connection" in self.__dict__:
            await (await self.connection).close()
