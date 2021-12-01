from pathlib import Path

import asyncstdlib as alib
import pytest

from saturn_engine.core import TopicMessage
from saturn_engine.worker.topics import FileTopic


@pytest.mark.asyncio
async def test_file_topic(tmp_path: Path) -> None:
    path = tmp_path / "topic.json"
    messages = [
        TopicMessage(id="0", args={"n": 1}),
        TopicMessage(id="1", args={"n": 2}),
    ]

    topic = FileTopic.from_options({"path": str(path), "mode": "w"})
    for message in messages:
        await topic.publish(message, wait=True)
    await topic.close()

    topic = FileTopic.from_options({"path": str(path), "mode": "r"})
    items = await alib.list(topic.run())
    assert items == messages
    await topic.close()
