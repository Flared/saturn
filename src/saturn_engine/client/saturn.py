from typing import Any
from typing import Coroutine
from typing import Optional
from typing import TypeVar

import asyncio

import aiohttp

from saturn_engine.config import Config
from saturn_engine.core import TopicMessage
from saturn_engine.core.api import TopicItem
from saturn_engine.core.api import TopicsResponse
from saturn_engine.utils import urlcat
from saturn_engine.utils.options import fromdict
from saturn_engine.worker import work_factory
from saturn_engine.worker.services.http_client import HttpClient
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.topic import Topic

T = TypeVar("T")


class SaturnClient:
    def __init__(
        self,
        *,
        services_manager: ServicesManager,
        topic_definitions: list[TopicItem],
    ) -> None:
        self.services_manager = services_manager
        self.services = services_manager.services
        self.topic_definitions = {
            topic_item.name: topic_item for topic_item in topic_definitions
        }
        self.topics: dict[str, Topic] = {}

    async def publish(self, topic_name: str, message: TopicMessage, wait: bool) -> bool:
        if topic_name not in self.topics:
            self.topics[topic_name] = work_factory.build_topic(
                self.topic_definitions[topic_name],
                services=self.services,
            )
        return await self.topics[topic_name].publish(message, wait)

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
    ) -> list[TopicItem]:
        topic_definitions_url = urlcat(base_url, "api/topics")
        async with http_client.get(topic_definitions_url) as response:
            return fromdict(await response.json(), TopicsResponse).items


class SyncSaturnClient:
    def __init__(
        self,
        config: Config,
        *,
        http_client: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self.loop = asyncio.new_event_loop()
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
        return cls(
            config=config,
            http_client=http_client,
        )

    def publish(self, topic_name: str, message: TopicMessage, wait: bool) -> bool:
        return self._run_sync(self._client.publish(topic_name, message, wait))

    def _run_sync(self, coroutine: Coroutine[Any, Any, T]) -> T:
        return self.loop.run_until_complete(coroutine)
