import asyncio


class Parkers:
    """Parkers allow multiple parker to lock a queue.

    A queue will only unlock once all parker have been unparked. It could happen
    in the case where a pipeline has an item waiting on resource (Blocking the
    queue) and another waiting to publish on an upstream queue (Also blocking
    the queue). Both item might have been dequeued before either block the
    queue. So the parkers keep track of both items and won't resume processing
    of the queue until both are done.
    """

    def __init__(self) -> None:
        self.condition = asyncio.Condition()
        self.parkers: set[object] = set()

    def park(self, parker: object) -> None:
        self.parkers.add(parker)

    async def unpark(self, parker: object) -> None:
        async with self.condition:
            self.parkers.discard(parker)
            self.condition.notify()

    def locked(self) -> bool:
        return bool(self.parkers)

    async def wait(self) -> None:
        """Wait until no parker are left. If no parkers, return right away."""
        async with self.condition:
            await self.condition.wait_for(lambda: not self.parkers)
