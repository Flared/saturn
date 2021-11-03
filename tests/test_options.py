import dataclasses
from typing import Union

from saturn_engine.utils.options import ObjectUnion
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import field
from saturn_engine.utils.options import fromdict


@dataclasses.dataclass
class NestedObjectA:
    fielda: str


@dataclasses.dataclass
class NestedObjectB:
    fieldb: str


@dataclasses.dataclass
class Object:
    nested: Union[NestedObjectA, NestedObjectB] = field(
        ObjectUnion(union={"a": NestedObjectA, "b": NestedObjectB})
    )


def test_object_union() -> None:
    assert asdict(Object(nested=NestedObjectA(fielda="a"))) == {
        "nested": {"a": {"fielda": "a"}}
    }
    assert asdict(Object(nested=NestedObjectB(fieldb="b"))) == {
        "nested": {"b": {"fieldb": "b"}}
    }
    assert fromdict({"nested": {"a": {"fielda": "a"}}}, Object) == Object(
        nested=NestedObjectA(fielda="a")
    )
    assert fromdict({"nested": {"b": {"fieldb": "b"}}}, Object) == Object(
        nested=NestedObjectB(fieldb="b")
    )
