from saturn_engine.core.api import PipelineInfo
from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import QueuePipeline
from saturn_engine.core.api import ResourceItem
from saturn_engine.core.api import SyncResponse


class WorkerManagerClient:
    def __init__(self) -> None:
        pass

    async def sync(self) -> SyncResponse:
        return SyncResponse(
            items=[
                QueueItem(
                    id="q-1",
                    pipeline=QueuePipeline(
                        info=PipelineInfo(
                            name="saturn_engine.examples.hello",
                            resources={},
                        ),
                        args={"who": "world"},
                    ),
                    options={"queue_name": "q1"},
                ),
                QueueItem(
                    id="q-2",
                    pipeline=QueuePipeline(
                        info=PipelineInfo(
                            name="saturn_engine.examples.foobar",
                            resources={"api_key": "FoobarApiKey"},
                        ),
                        args={},
                    ),
                    options={"queue_name": "q2"},
                ),
            ],
            resources=[
                ResourceItem(
                    id="r-1",
                    type="FoobarApiKey",
                    data={"key": "foobar-XXXXXXXX"},
                )
            ],
        )
