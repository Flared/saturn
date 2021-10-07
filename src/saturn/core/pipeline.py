from saturn.core import Message


async def echo(message: Message) -> None:
    print(message)
