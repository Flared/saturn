import aio_pika
import aio_pika.abc

from saturn_engine.utils.asyncutils import cached_property
from saturn_engine.utils.log import getLogger

from . import MinimalService


class RabbitMQService(MinimalService):
    name = "rabbitmq"

    async def open(self) -> None:
        self.logger = getLogger(__name__, self)

    @cached_property
    async def connection(self) -> aio_pika.abc.AbstractRobustConnection:
        connection = await aio_pika.connect_robust(self.services.config.c.rabbitmq.url)
        connection.reconnect_callbacks.add(self.connection_reconnect)
        connection.close_callbacks.add(self.connection_closed)
        return connection

    async def close(self) -> None:
        if "connection" in self.__dict__:
            await (await self.connection).close()

    def connection_closed(
        self, connection: aio_pika.abc.AbstractRobustConnection, reason: object
    ) -> None:
        extra = {"data": {"connection": str(connection)}}
        if isinstance(reason, BaseException):
            self.logger.error("Channel closed", exc_info=reason, extra=extra)
        elif reason:
            self.logger.error("Channel closed: %s", reason, extra=extra)

    def connection_reconnect(
        self, connection: aio_pika.abc.AbstractRobustConnection
    ) -> None:
        self.logger.info(
            "Connection reopening", extra={"data": {"connection": str(connection)}}
        )
