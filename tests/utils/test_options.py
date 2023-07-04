import typing as t

import dataclasses
from datetime import datetime

import pydantic
import pytest

from saturn_engine.utils.inspect import get_import_name
from saturn_engine.utils.options import OptionsSchema
from saturn_engine.utils.options import SymbolName
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import fromdict
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


@dataclasses.dataclass
class BetterObject(Object):
    z: NestedObjectA


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


def test_inheritance() -> None:
    a = fromdict({"x": "foo", "y": "2020-01-01T01:01:01"}, Object)
    assert a == Object(x="foo", y=datetime(2020, 1, 1, 1, 1, 1))
    b = fromdict(
        {"x": "foo", "y": "2020-01-01T01:01:01", "z": {"fielda": "foo"}}, BetterObject
    )
    assert b == BetterObject(
        x="foo", y=datetime(2020, 1, 1, 1, 1, 1), z=NestedObjectA(fielda="foo")
    )


class FooBar:
    def __init__(self, x: int) -> None:
        ...

    def biz(self) -> None:
        ...


def foobar(x: int) -> FooBar:
    return FooBar(1)


def test_symbol_name() -> None:
    @dataclasses.dataclass
    class ObjectWithSymbol:
        x: SymbolName

    x = fromdict({"x": "builtins.int"}, ObjectWithSymbol)
    assert x.x is int

    x = fromdict({"x": int}, ObjectWithSymbol)
    assert x.x is int

    @dataclasses.dataclass
    class ObjectWithTypedSymbol:
        x: SymbolName[t.Type[Object]]

    with pytest.raises(pydantic.ValidationError):
        fromdict({"x": "builtins.int"}, ObjectWithTypedSymbol)

    y = fromdict({"x": get_import_name(Object)}, ObjectWithTypedSymbol)
    assert y.x is Object

    @dataclasses.dataclass
    class ObjectWithCallable:
        x: SymbolName[t.Callable[[int], FooBar]]

    with pytest.raises(pydantic.ValidationError):
        fromdict({"x": "builtins.False"}, ObjectWithCallable)

    z = fromdict({"x": get_import_name(FooBar)}, ObjectWithCallable)
    assert z.x is FooBar
    z = fromdict({"x": get_import_name(foobar)}, ObjectWithCallable)
    assert z.x is foobar
