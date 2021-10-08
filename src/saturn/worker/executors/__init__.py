from saturn.core.message import Message


class BaseExecutor:
    def __init__(self) -> None:
        pass

    async def submit(self, message: Message) -> None:
        pass
