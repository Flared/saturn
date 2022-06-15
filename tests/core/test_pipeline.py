from typing import Optional

import dataclasses

from saturn_engine.core.pipeline import PipelineInfo


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
