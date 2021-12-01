from typing import Any
from saturn_engine.core import TopicMessage

def echo(**kwargs: Any):
    return TopicMessage(args=kwargs)
