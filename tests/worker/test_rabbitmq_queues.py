import asyncio
from collections.abc import Iterator

import asyncstdlib as alib
import pytest

from saturn_engine.core import TopicMessage
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.services.rabbitmq import RabbitMQService
from saturn_engine.worker.topics.rabbitmq import RabbitMQTopic


@pytest.fixture
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """Restore original pytest event loop"""
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
async def test_rabbitmq_publish(
    rabbitmq_service: RabbitMQService, services_manager: ServicesManager
) -> None:
    # Create our test queue and publisher
    queue1 = RabbitMQTopic(
        RabbitMQTopic.Options("test_queue1", False, True), services_manager.services
    )
    queue2 = RabbitMQTopic(
        RabbitMQTopic.Options("test_queue2", False, True), services_manager.services
    )
    publisher1 = RabbitMQTopic(
        RabbitMQTopic.Options("test_queue1", False, True), services_manager.services
    )
    publisher2 = RabbitMQTopic(
        RabbitMQTopic.Options("test_queue2", False, True), services_manager.services
    )

    # Create an asyncgenerator for our messages
    queue1generator = queue1.run()
    queue2generator = queue2.run()

    # Publish our test messages on both queues and test them
    for i in range(10):
        await publisher1.publish(TopicMessage(args={"id": i, "none": None}), wait=True)
        await publisher2.publish(TopicMessage(args={"id": i}), wait=True)
        item = await alib.anext(queue1generator)
        async with item as message:
            assert message.args["id"] == i

    # Test the second queue
    for i in range(10):
        item = await alib.anext(queue2generator)
        async with item as message:
            assert message.args["id"] == i

    # Close our connections
    await queue1generator.aclose()
    await queue2generator.aclose()
