import asyncio
import time

import pytest

from saturn_engine.client.saturn import SaturnClient
from saturn_engine.client.saturn import SyncSaturnClient
from saturn_engine.config import Config
from saturn_engine.core import TopicMessage
from saturn_engine.utils.inspect import get_import_name
from saturn_engine.worker.topics.memory import MemoryTopic
from saturn_engine.worker.topics.memory import get_queue
from saturn_engine.worker_manager.config.declarative import load_definitions_from_str
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions
from tests.utils import HttpClientMock


class DelayedMemoryTopic(MemoryTopic):
    async def publish(self, message: TopicMessage, wait: bool) -> bool:
        loop = asyncio.get_running_loop()
        asyncio.run_coroutine_threadsafe(super().publish(message, wait), loop)
        return True


def test_saturn_client_publish_sync(
    config: Config,
    http_client_mock: HttpClientMock,
    static_definitions: StaticDefinitions,
) -> None:
    http_client_mock.get("http://localhost:5000/api/topics").return_value = {
        "items": [
            {
                "name": "test-topic",
                "options": {},
                "type": get_import_name(DelayedMemoryTopic),
            }
        ]
    }

    saturn_client = SyncSaturnClient.from_config(
        config=config,
        http_client=http_client_mock.client(),
    )
    assert saturn_client.publish("test-topic", TopicMessage({"a": 0}), True)
    queue = get_queue("test-topic")
    assert queue.qsize() == 0

    # Racy, but 0.1 should be more than enough to let the background task to
    # run.
    time.sleep(0.1)

    assert queue.get_nowait().args["a"] == 0
    queue.task_done()
    assert queue.qsize() == 0

    with pytest.raises(KeyError):
        saturn_client.publish("test-topic2", TopicMessage({"a": 0}), True)
    assert queue.qsize() == 0

    saturn_client.close()
    assert not saturn_client._loop_thread.is_alive()


@pytest.mark.asyncio
async def test_saturn_client_publish_async(
    config: Config,
    http_client_mock: HttpClientMock,
    static_definitions: StaticDefinitions,
) -> None:
    http_client_mock.get("http://localhost:5000/api/topics").return_value = {
        "items": [{"name": "test-topic", "options": {}, "type": "MemoryTopic"}]
    }
    static_definitions.topics = load_definitions_from_str(
        """
apiVersion: saturn.flared.io/v1alpha1
kind: SaturnTopic
metadata:
  name: test-topic
spec:
  type: MemoryTopic
  options: {}
---
"""
    ).topics

    saturn_client = await SaturnClient.from_config(
        config,
        http_client=http_client_mock.client(),
    )
    assert await saturn_client.publish("test-topic", TopicMessage({"a": 0}), True)

    with pytest.raises(KeyError):
        assert await saturn_client.publish("test-topic2", TopicMessage({"a": 0}), True)

    queue = get_queue("test-topic")
    assert queue.get_nowait().args["a"] == 0
    queue.task_done()
    assert queue.qsize() == 0
    await saturn_client.close()
