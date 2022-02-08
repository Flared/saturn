import pytest

from saturn_engine.client.saturn import SaturnClient
from saturn_engine.client.saturn import SyncSaturnClient
from saturn_engine.config import Config
from saturn_engine.core import TopicMessage
from saturn_engine.worker.topics.memory import get_queue
from saturn_engine.worker_manager.config.declarative import load_definitions_from_str
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions
from tests.utils import HttpClientMock


def test_saturn_client_publish_sync(
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

    saturn_client = SyncSaturnClient.from_config(
        config=config,
        http_client=http_client_mock.client(),
    )
    assert saturn_client.publish("test-topic", TopicMessage({"a": 0}), True)

    with pytest.raises(KeyError):
        saturn_client.publish("test-topic2", TopicMessage({"a": 0}), True)

    queue = get_queue("test-topic")
    assert queue.get_nowait().args["a"] == 0
    queue.task_done()
    assert queue.qsize() == 0
    saturn_client.close()


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
