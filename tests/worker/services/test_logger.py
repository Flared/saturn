import typing as t

import dataclasses
import logging

import pytest

from saturn_engine.core import PipelineInfo
from saturn_engine.core import PipelineOutput
from saturn_engine.core import PipelineResults
from saturn_engine.core import Resource
from saturn_engine.core import ResourceUsed
from saturn_engine.core import TopicMessage
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.services.loggers.logger import Logger
from saturn_engine.worker.services.manager import ServicesManager


@dataclasses.dataclass(eq=False)
class FakeResource(Resource):
    data: str


def fake_pipeline(x: int, r: FakeResource) -> None:
    pass


@pytest.mark.asyncio
async def test_logger_message_executed(
    services_manager: ServicesManager,
    caplog: t.Any,
    executable_maker: t.Callable[..., ExecutableMessage],
) -> None:
    logger = services_manager._load_service(Logger)
    await logger.open()

    pipeline_info = PipelineInfo.from_pipeline(fake_pipeline)
    xmsg = executable_maker(pipeline_info=pipeline_info)
    xmsg.message.message = TopicMessage(id="m1", args={"x": 42})
    xmsg.message.update_with_resources(
        {FakeResource._typename(): {"name": "r1", "data": "foobar"}}
    )

    results = PipelineResults(
        outputs=[
            PipelineOutput(
                channel="default", message=TopicMessage(id="m2", args={"foo": "bar"})
            )
        ],
        resources=[ResourceUsed(type=FakeResource._typename(), release_at=10)],
    )

    with caplog.at_level(logging.DEBUG):
        hook_generator = logger.on_message_executed(xmsg)
        await hook_generator.__anext__()
        r = caplog.records[-1]
        assert r.message == "Executing message"
        assert r.data == {
            "input": "fake-topic",
            "job": "fake-queue",
            "message": {
                "id": "m1",
                "tags": {},
            },
            "resources": {FakeResource._typename(): "r1"},
            "pipeline": "tests.worker.services.test_logger.fake_pipeline",
        }

        with pytest.raises(StopAsyncIteration):
            await hook_generator.asend(results)

        r = caplog.records[-1]
        assert r.message == "Executed message"
        assert r.data == {
            "input": "fake-topic",
            "job": "fake-queue",
            "message": {
                "id": "m1",
                "tags": {},
            },
            "resources": {FakeResource._typename(): "r1"},
            "pipeline": "tests.worker.services.test_logger.fake_pipeline",
            "result": {
                "output": [
                    {
                        "channel": "default",
                        "message": {"id": "m2", "tags": {}},
                    }
                ],
                "resources": {FakeResource._typename(): {"release_at": 10}},
            },
        }
