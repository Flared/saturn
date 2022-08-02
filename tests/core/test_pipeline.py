from typing import Optional

import dataclasses

from saturn_engine.core import Resource
from saturn_engine.core.pipeline import PipelineInfo
from saturn_engine.core.pipeline import ResourceUsed


def test_instancify_args_optional() -> None:
    @dataclasses.dataclass
    class A:
        field: str

    def pipeline(a: Optional[A]) -> None:
        pass

    args: dict[str, object] = {"a": {"field": "value"}}

    PipelineInfo.instancify_args(
        args=args,
        pipeline=pipeline,
    )

    assert args == {"a": A(field="value")}


def test_instancify_args_list() -> None:
    @dataclasses.dataclass
    class Carrot:
        color: str

    def pipeline(carrots: list[Carrot]) -> None:
        pass

    args: dict[str, object] = {
        "carrots": [
            {"color": "orange"},
            {"color": "purple"},
        ]
    }

    PipelineInfo.instancify_args(
        args=args,
        pipeline=pipeline,
    )

    assert args == {
        "carrots": [
            Carrot(color="orange"),
            Carrot(color="purple"),
        ]
    }


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
