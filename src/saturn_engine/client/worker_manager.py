from saturn_engine.core.api import QueueItem
from saturn_engine.core.api import SyncResponse


class WorkerManagerClient:
    def __init__(self) -> None:
        pass

    async def sync(self) -> SyncResponse:
        return SyncResponse(
            items=[
                QueueItem(
                    id="q-1",
                    pipeline="hello",
                    ressources=[],
                    options={"queue_name": "q1"},
                ),
                QueueItem(
                    id="q-2",
                    pipeline="hello",
                    ressources=[],
                    options={"queue_name": "q2"},
                ),
            ]
        )
