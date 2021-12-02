from typing import Any

from saturn_engine.core import TopicMessage


def echo(**kwargs: Any) -> TopicMessage:
    print("echo: ", kwargs)
    return TopicMessage(args=kwargs)
