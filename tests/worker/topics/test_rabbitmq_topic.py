import asyncstdlib as alib
import pytest

from saturn_engine.core import TopicMessage
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.services.rabbitmq import RabbitMQService
from saturn_engine.worker.topics import RabbitMQTopic
from tests.utils import TimeForwardLoop


@pytest.mark.asyncio
async def test_rabbitmq_topic(
    event_loop: TimeForwardLoop,
    rabbitmq_service: RabbitMQService,
    services_manager: ServicesManager,
) -> None:
    event_loop.forward_time = False

    options = RabbitMQTopic.Options(
        queue_name="test", auto_delete=True, persistent=False
    )
    topic = RabbitMQTopic(options, services=services_manager.services)

    messages = [
        TopicMessage(id="0", args={"n": 1}),
        TopicMessage(id="1", args={"n": 2}),
    ]

    for message in messages:
        await topic.publish(message, wait=True)

    async with alib.scoped_iter(topic.run()) as topic_iter:
        items = []
        async for context in alib.islice(topic_iter, 2):
            async with context as message:
                items.append(message)
        assert items == messages

    await topic.close()
