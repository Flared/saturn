import typing as t

import time

from saturn_engine.core import TopicMessage

from .resources import BackpressureApiKey
from .resources import TestApiKey


def echo(api_key: TestApiKey, **kwargs: t.Any) -> TopicMessage:
    print("api_key:", api_key.key, "data:", kwargs)
    return TopicMessage(args=kwargs)


def slow(api_key: BackpressureApiKey, **kwargs: t.Any) -> TopicMessage:
    time.sleep(10)
    return TopicMessage(args=kwargs)


def fast(**kwargs: t.Any) -> TopicMessage:
    return TopicMessage(args=kwargs)
