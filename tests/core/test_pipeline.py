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
