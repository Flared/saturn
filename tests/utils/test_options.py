import dataclasses
from datetime import datetime

from saturn_engine.utils.options import OptionsSchema
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import json_serializer


@dataclasses.dataclass
class NestedObjectA:
    fielda: str


@dataclasses.dataclass
class NestedObjectB:
    fieldb: str


@dataclasses.dataclass
class Object:
    x: str
    y: datetime


class FromObject(OptionsSchema):
    class Options(Object):
        pass

    def __init__(self, options: Options) -> None:
        self.options = options


def test_from_as_dict() -> None:
    assert asdict(
        FromObject.from_options({"x": "123", "y": "2023-01-01T10:10:10"}).options
    ) == {
        "x": "123",
        "y": datetime(2023, 1, 1, 10, 10, 10),
    }


def test_json_serializer() -> None:
    assert (
        json_serializer({"x": datetime(2023, 1, 1, 10, 10, 10)})
        == '{"x": "2023-01-01T10:10:10"}'
    )
