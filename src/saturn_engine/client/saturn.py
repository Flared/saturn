from typing import Any
from typing import Coroutine
from typing import Optional
from typing import TypeVar

import asyncio
import concurrent.futures
import threading

import aiohttp

from saturn_engine.config import Config
from saturn_engine.core import TopicMessage
from saturn_engine.core.api import ComponentDefinition
from saturn_engine.core.api import TopicsResponse
from saturn_engine.utils import LONG_TIMEOUT
from saturn_engine.utils import MEDIUM_TIMEOUT
from saturn_engine.utils import urlcat
from saturn_engine.utils.log import getLogger
from saturn_engine.utils.options import fromdict
from saturn_engine.worker.services.http_client import HttpClient
from saturn_engine.worker.services.loggers.logger import topic_message_data
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.topic import Topic

T = TypeVar("T")


class SaturnClient:
    def __init__(
        self,
        *,
        services_manager: ServicesManager,
        topic_definitions: list[ComponentDefinition],
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.services_manager = services_manager
        self.services = services_manager.services
        self.topic_definitions = {
            topic_item.name: topic_item for topic_item in topic_definitions
        }
        self.topics: dict[str, Topic] = {}

    async def publish(self, topic_name: str, message: TopicMessage, wait: bool) -> bool:
        from saturn_engine.worker import work_factory

        publish_info = {
            "topic": topic_name,
            "message": topic_message_data(message),
        }
        try:
            if topic_name not in self.topics:
                self.topics[topic_name] = work_factory.build_topic(
                    self.topic_definitions[topic_name],
                    services=self.services,
                )
            self.logger.debug("Publishing message", extra={"data": publish_info})
            return await self.topics[topic_name].publish(message, wait)
        except Exception:
            self.logger.exception(
                "Failed to publish message", extra={"data": publish_info}
            )
            raise

    async def close(self) -> None:
        await self.services_manager.close()

    @classmethod
    async def from_config(
        cls,
        config: Config,
        *,
        http_client: Optional[aiohttp.ClientSession] = None,
    ) -> "SaturnClient":
        services_manager = ServicesManager(config)
        await services_manager.open()
        services = services_manager.services
        topic_definitions = await cls._load_topic_definitions(
            http_client=http_client or services.cast_service(HttpClient).session,
            base_url=services.s.config.c.worker_manager_url,
        )
        return cls(
            services_manager=services_manager,
            topic_definitions=topic_definitions,
        )

    @staticmethod
    async def _load_topic_definitions(
        *,
        http_client: aiohttp.ClientSession,
        base_url: str,
    ) -> list[ComponentDefinition]:
        topic_definitions_url = urlcat(base_url, "api/topics")
        async with http_client.get(topic_definitions_url) as response:
            return fromdict(await response.json(), TopicsResponse).items


class SyncSaturnClient:
    _instance = None
    _instance_lock = threading.Lock()

    def __init__(
        self,
        config: Config,
        *,
        http_client: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self._loop: asyncio.AbstractEventLoop
        self._loop_initialized = threading.Event()
        self._loop_thread = threading.Thread(
            target=self._run_loop, name="saturn-client", daemon=True
        )
        self._loop_thread.start()

        if not self._loop_initialized.wait(timeout=MEDIUM_TIMEOUT):
            raise RuntimeError("Client intializing timed out")

        self._client = self._run_sync(
            SaturnClient.from_config(
                config=config,
                http_client=http_client,
            )
        )

    @classmethod
    def from_config(
        cls,
        config: Config,
        *,
        http_client: Optional[aiohttp.ClientSession] = None,
    ) -> "SyncSaturnClient":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls(
                        config=config,
                        http_client=http_client,
                    )
        return cls._instance

    def publish(
        self,
        topic_name: str,
        message: TopicMessage,
        wait: bool,
        *,
        timeout: Optional[float] = MEDIUM_TIMEOUT,
    ) -> bool:
        return self._run_sync(
            self._client.publish(topic_name, message, wait), timeout=timeout
        )

    def _run_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._loop_initialized.set()
        self._loop.run_forever()
        self._loop.close()

    def _run_sync(
        self,
        coroutine: Coroutine[Any, Any, T],
        timeout: Optional[float] = MEDIUM_TIMEOUT,
    ) -> T:
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise

    def close(self) -> None:
        try:
            self._run_sync(self._client.close(), timeout=LONG_TIMEOUT)
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._loop_thread.join(timeout=LONG_TIMEOUT)
