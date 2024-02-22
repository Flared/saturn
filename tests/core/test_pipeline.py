import typing as t

import dataclasses
import pickle  # noqa: S403
from unittest.mock import Mock

from saturn_engine.core import Resource
from saturn_engine.core.pipeline import CancellationToken
from saturn_engine.core.pipeline import PipelineInfo
from saturn_engine.core.pipeline import ResourceUsed
from saturn_engine.core.topic import TopicMessage
from saturn_engine.worker.pipeline_message import PipelineMessage


class FakeResource1(Resource):
    pass


class FakeResource2(Resource):
    typename = "FakeResource2"


def test_from_resource() -> None:
    assert ResourceUsed.from_resource(
        FakeResource1(name="fake-1"), release_at=42
    ) == ResourceUsed(type="tests.core.test_pipeline.FakeResource1", release_at=42)
    assert ResourceUsed.from_resource(
        FakeResource2(name="fake-2"), release_at=123
    ) == ResourceUsed(type="FakeResource2", release_at=123)


@dataclasses.dataclass
class A:
    a: str


@dataclasses.dataclass
class B:
    b: str


class MetaType:
    pass


# This is not a dataclass!
class C(MetaType):
    __slots__ = "c"

    def __init__(self, c: str) -> None:
        self.c = c


def pipeline(a: t.Optional[A], b: list[B], c: C, d: str = "d", *, spy: Mock) -> int:
    spy(a=a, b=b, c=c, d=d)
    return 1


def pipeline_kwargs(a: int, *, spy: Mock, **kwargs: t.Any) -> int:
    spy(a=a, **kwargs)
    return 1


def test_pipeline_messages_execute() -> None:
    spy = Mock()
    c = C("c")
    args: dict = {
        "a": {"a": "a"},
        "b": [{"b": "b"}],
        "e": "e",
        "spy": spy,
    }

    msg = PipelineMessage(
        info=PipelineInfo.from_pipeline(pipeline),
        message=TopicMessage(args=args),
    )
    msg.set_meta_arg(meta_type=MetaType, value=c)
    assert msg.execute() == 1

    spy.assert_called_once_with(
        a=A(a="a"),
        b=[B(b="b")],
        c=c,
        d="d",
    )


def test_pipeline_kwargs_execute() -> None:
    spy = Mock()
    c = C("c")
    args: dict = {
        "a": 1,
        "b": 2,
        "spy": spy,
    }

    msg = PipelineMessage(
        info=PipelineInfo.from_pipeline(pipeline_kwargs),
        message=TopicMessage(args=args),
    )
    msg.set_meta_arg(meta_type=MetaType, value=c)
    assert msg.execute() == 1

    spy.assert_called_once_with(
        a=1,
        b=2,
    )


def test_cancellation_oken_serialization() -> None:
    token = CancellationToken()

    new_token = pickle.loads(pickle.dumps(token))  # noqa: S301
    assert not new_token.is_cancelled
    new_token._cancel()

    new_token = pickle.loads(pickle.dumps(new_token))  # noqa: S301
    assert new_token.is_cancelled
