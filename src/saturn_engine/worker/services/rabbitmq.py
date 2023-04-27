import aio_pika
import aio_pika.abc

from saturn_engine.utils.asyncutils import AsyncLazyDict
from saturn_engine.utils.log import getLogger

from . import BaseServices
from . import Service


class RabbitMQService(Service[BaseServices, "RabbitMQService.Options"]):
    connections: AsyncLazyDict[str, aio_pika.abc.AbstractRobustConnection]

    name = "rabbitmq"
    DEFAULT_NAME = "default"

    class Options:
        url: str
        urls: dict[str, str]

    async def open(self) -> None:
        self.logger = getLogger(__name__, self)
        self.options.urls.setdefault("default", self.options.url)
        self.connections = AsyncLazyDict(self._connect)

    async def _connect(self, name: str) -> aio_pika.abc.AbstractRobustConnection:
        url = self.options.urls[name]
        connection = await aio_pika.connect_robust(url)
        connection.reconnect_callbacks.add(self.connection_reconnect)
        connection.close_callbacks.add(self.connection_closed)
        return connection

    async def close(self) -> None:
        for connection in self.connections:
            await (await connection).close()
        self.connections.clear()

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
