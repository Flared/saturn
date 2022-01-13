from typing import Any

from saturn_engine.core import TopicMessage

from .resources import TestApiKey


def echo(api_key: TestApiKey, **kwargs: Any) -> TopicMessage:
    print("api_key:", api_key.key, "data:", kwargs)
    return TopicMessage(args=kwargs)
