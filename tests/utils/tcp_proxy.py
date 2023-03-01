import typing as t

import asyncio
import contextlib


# Adapted from https://github.com/aio-libs/aiopg/blob/master/tests/conftest.py#L416
class TcpProxy:
    """
    TCP proxy. Allows simulating connection breaks in tests.
    """

    MAX_BYTES = 1024

    def __init__(
        self,
        *,
        src_host: str = "127.0.0.1",
        src_port: int,
        dst_host: str = "127.0.0.1",
        dst_port: int,
    ) -> None:
        self.src_host = src_host
        self.src_port = src_port
        self.dst_host = dst_host
        self.dst_port = dst_port
        self.connections: set[t.Any] = set()
        self.server: t.Optional[asyncio.AbstractServer] = None

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self._handle_client,
            host=self.src_host,
            port=self.src_port,
        )

    async def disconnect(self) -> None:
        while self.connections:
            writer = self.connections.pop()
            writer.close()
            if hasattr(writer, "wait_closed"):
                await writer.wait_closed()

    @staticmethod
    async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while not reader.at_eof():
                bytes_read = await reader.read(TcpProxy.MAX_BYTES)
                writer.write(bytes_read)
                await writer.drain()
        finally:
            writer.close()

    async def _handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        server_reader, server_writer = await asyncio.open_connection(
            host=self.dst_host, port=self.dst_port
        )

        self.connections.add(server_writer)
        self.connections.add(client_writer)

        done, _ = await asyncio.wait(
            [
                asyncio.ensure_future(self._pipe(server_reader, client_writer)),
                asyncio.ensure_future(self._pipe(client_reader, server_writer)),
            ]
        )
        for fut in done:
            with contextlib.suppress(Exception):
                await fut

    async def close(self) -> None:
        await self.disconnect()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
