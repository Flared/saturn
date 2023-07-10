import typing as t

import dataclasses
from unittest.mock import Mock

from saturn_engine.utils.inspect import dataclass_from_params


@dataclasses.dataclass
class Foo:
    x: int


class Bar:
    pass


def test_dataclass_from_params() -> None:
    spy = Mock()

    def func(
        x: t.Annotated[int, Bar],
        y: t.Optional[str] = None,
        *args: t.Any,
        z: Foo,
        **kwargs: t.Any
    ) -> None:
        spy(*args, x=x, y=y, z=z, **kwargs)

    dataclass = dataclass_from_params(func)
    args = dataclass(x=1, z=Foo(x=1))
    args.call(kwargs={"foo": "bar"})
    spy.assert_called_once_with(
        x=1,
        y=None,
        z=Foo(x=1),
        foo="bar",
    )

    assert dataclass.find_by_type(str) == "y"
    assert dataclass.find_by_type(Foo) == "z"
    assert dataclass.find_by_type(Bar) == "x"
    assert dataclass.find_by_type(float) is None
