from typing import Optional

import asyncio

from saturn_engine.config import default_config_with_env
from saturn_engine.utils.options import asdict
from saturn_engine.worker import work_factory
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions

from .config.topic_test import TopicTest
from .diff import get_diff


def run_saturn_topic(
    *,
    static_definitions: StaticDefinitions,
    topic_name: str,
    limit: Optional[int] = None,
    skip: Optional[int] = None,
) -> list[dict]:
    topic_item = static_definitions.topics[topic_name]
    topic = work_factory.build_topic(
        topic_item=topic_item,
        services=ServicesManager(
            config=default_config_with_env(),
        ).services,
    )
    messages: list[dict] = []

    async def run_topic() -> None:
        count = 0
        skipped = 0
        async for message in topic.run():
            async with message:
                if skip is not None and skipped < skip:
                    skipped += 1
                    continue

                messages.append(asdict(message))
                count = count + 1
                if limit and count >= limit:
                    break

    asyncio.run(run_topic())

    return messages


def run_saturn_topic_test(
    *,
    static_definitions: StaticDefinitions,
    topic_test: TopicTest,
) -> None:
    messages: list[dict] = run_saturn_topic(
        static_definitions=static_definitions,
        topic_name=topic_test.spec.selector.topic,
        limit=topic_test.spec.limit,
        skip=topic_test.spec.skip,
    )

    expected_messages: list[dict] = [asdict(item) for item in topic_test.spec.messages]

    if messages != expected_messages:
        diff: str = get_diff(
            expected=expected_messages,
            got=messages,
        )
        raise AssertionError(
            f"Topic messages do not match the expected messages:\n{diff}"
        )
