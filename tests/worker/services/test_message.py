import typing as t

from unittest.mock import Mock

import pytest

from saturn_engine.core.pipeline import PipelineInfo
from saturn_engine.core.pipeline import PipelineOutput
from saturn_engine.core.topic import TopicMessage
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker.services import BaseServices
from saturn_engine.worker.services.hooks import MessagePublished
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker.services.message import MessageTagger


@pytest.mark.asyncio
async def test_message_tagger(services_manager: ServicesManager) -> None:
    message_tagger = MessageTagger(t.cast(BaseServices, services_manager.services))

    parent_message = TopicMessage(args={"n": 1})
    output_message = TopicMessage(args={"n": 2})

    message_published = MessagePublished(
        message=PipelineMessage(
            info=PipelineInfo(name="test", resources={}),
            message=parent_message,
        ),
        topic=Mock(),
        output=PipelineOutput(
            channel="test",
            message=output_message,
        ),
    )

    hook_generator = message_tagger.on_message_published(message_published)
    await hook_generator.asend(None)

    assert message_published.output.message.tags == {
        "parent_message_id": parent_message.id,
        "root_message_id": parent_message.id,
    }
