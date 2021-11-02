from saturn_engine.core.api import PipelineInfo
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import QueuePipeline
from saturn_engine.core.api import ResourceItem
from saturn_engine.core.api import SyncResponse
from saturn_engine.core.api import TopicItem


class WorkerManagerClient:
    def __init__(self) -> None:
        pass

    async def sync(self) -> SyncResponse:
        return SyncResponse(
            items=[
                QueueItem(
                    name="q-1",
                    input=TopicItem(
                        name="t-1", type="RabbitMQ", options={"queue_name": "q1"}
                    ),
                    pipeline=QueuePipeline(
                        info=PipelineInfo(
                            name="saturn_engine.examples.hello",
                            resources={},
                        ),
                        args={"who": "world"},
                    ),
                    output={
                        "default": [
                            TopicItem(
                                name="t-3",
                                type="Stdout",
                            )
                        ]
                    },
                ),
                QueueItem(
                    name="q-2",
                    input=TopicItem(
                        name="t-2", type="RabbitMQ", options={"queue_name": "q2"}
                    ),
                    pipeline=QueuePipeline(
                        info=PipelineInfo(
                            name="saturn_engine.examples.foobar",
                            resources={"api_key": "FoobarApiKey"},
                        ),
                        args={},
                    ),
                    output={
                        "default": [
                            TopicItem(
                                name="t-3",
                                type="Stdout",
                            )
                        ]
                    },
                ),
            ],
            resources=[
                ResourceItem(
                    name="r-1",
                    type="FoobarApiKey",
                    data={"key": "foobar-XXXXXXXX"},
                )
            ],
        )
