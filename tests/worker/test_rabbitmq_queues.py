import asyncio
from collections.abc import Iterator

import asyncstdlib as alib
import pytest

from saturn_engine.config import Config
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
@pytest.mark.rabbitmq
async def test_rabbitmq_publish(config: Config) -> None:
    # Create and open our services manager
    test_svc_mgr = ServicesManager(config)
    test_svc_mgr._load_service(RabbitMQService)

    await test_svc_mgr.open()

    # Create our test queue and publisher
    queue1 = RabbitMQTopic(
        RabbitMQTopic.Options("test_queue1", False, True), test_svc_mgr.services
    )
    queue2 = RabbitMQTopic(
        RabbitMQTopic.Options("test_queue2", False, True), test_svc_mgr.services
    )
    publisher1 = RabbitMQTopic(
        RabbitMQTopic.Options("test_queue1", False, True), test_svc_mgr.services
    )
    publisher2 = RabbitMQTopic(
        RabbitMQTopic.Options("test_queue2", False, True), test_svc_mgr.services
    )

    # Create an asyncgenerator for our messages
    queue1generator = queue1.run()
    queue2generator = queue2.run()

    # Publish our test messages on both queues and test them
    for i in range(10):
        await publisher1.publish(TopicMessage(args={"id": i}), wait=True)
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
    await test_svc_mgr.close()
