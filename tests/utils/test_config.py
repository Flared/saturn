import typing as t

import re

import pytest

from saturn_engine.config import Config as SaturnConfig
from saturn_engine.config import default_client_config
from saturn_engine.config import default_config
from saturn_engine.utils.config import Config
from saturn_engine.utils.config import LazyConfig

if t.TYPE_CHECKING:
    from mypy_typing_asserts import assert_type


class Interface:
    a: str
    b: int

    class c:
        x: str
        y: dict[str, list[int]]


class ExtraInterface:
    z: str
    y = 2


class ObjectConfig:
    a = "1"
    b = 2

    class c:
        x = "3"
        y = {"a": [4, 5]}

    class d:
        z = "6"

    e = "7"


def test_config() -> None:
    config: Config[Interface] = Config()
    config = config.load_object(ObjectConfig).register_interface("", Interface)

    class ExtraConfig:
        a = "111"

    config = config.load_objects([ExtraConfig, {"b": 3}])

    config = config.register_interface("d", ExtraInterface)

    # typed access through `.c`
    assert config.c.a == "111"
    assert config.c.b == 3
    assert config.c.c.x == "3"

    # Untyped, but item/attr and case insensitive access through `.r`
    assert t.cast(t.Any, config).r.D.Z == "6"
    assert t.cast(t.Any, config).r["d"]["Z"] == "6"

    d: ExtraInterface = config.cast_namespace("d", ExtraInterface)
    assert d.y == 2

    if t.TYPE_CHECKING:
        assert_type[Interface](config.c)


def test_any_config() -> None:
    class AnyConfig:
        a: t.Any

    config: Config[AnyConfig] = Config()
    config = config.load_object({"a": None}).register_interface("", AnyConfig)
    config.load_object({"a": 1})
    config.load_object({"a": "1"})


def test_optional_config() -> None:
    class OptionalConfig:
        a: t.Optional["list[int]"]

    config: Config[OptionalConfig] = Config()
    config = config.load_object({"a": None}).register_interface("", OptionalConfig)
    config.load_object({"a": [1]})
    with pytest.raises(ValueError):
        config.load_object({"a": ["1"]})


def test_union_config() -> None:
    class UnionConfig:
        a: t.Union["t.Optional[int]", str]

    config: Config[UnionConfig] = Config()
    config = config.load_object({"a": None}).register_interface("", UnionConfig)
    config.load_object({"a": 1})
    config.load_object({"a": "1"})
    with pytest.raises(ValueError):
        config.load_object({"a": ["1"]})


def test_config_error() -> None:
    config: Config[Interface] = Config()

    with pytest.raises(AttributeError):
        config.c.a

    config = config.load_object(ObjectConfig).register_interface("", Interface)

    with pytest.raises(
        ValueError,
        match='Invalid config key "a" type: expected "<class \'str\'>", got "int"',
    ):
        config.load_object({"a": 1})

    with pytest.raises(
        ValueError,
        match=re.escape(
            'Invalid config key "c.y" type: expected "dict[str, list[int]]", got "dict"'
        ),
    ):
        config.load_object({"c": {"y": {"1": ["a"]}}})


def test_lazy_config() -> None:
    config = LazyConfig()

    with pytest.raises(ValueError, match="Missing config key .*"):
        config.cast_namespace("c", Interface.c)

    config = config.load_object(ObjectConfig)
    assert config.cast_namespace("c", Interface.c).x == "3"
    extended_config = config.load_object({"c": {"x": "4"}})
    assert config.cast_namespace("c", Interface.c).x == "3"
    assert extended_config.cast_namespace("c", Interface.c).x == "4"

    if t.TYPE_CHECKING:
        assert_type[Interface.c](config.cast_namespace("c", Interface.c))

    config_2 = LazyConfig([ObjectConfig])
    assert config.cast_namespace("c", Interface.c).x == "3"
    extended_config_2 = config_2.load_object(extended_config)
    assert extended_config_2.cast_namespace("c", Interface.c).x == "4"


def test_default_config() -> None:
    # Test default_config can be loaded by itself.
    config = SaturnConfig().load_object(default_config)
    assert config


def test_default_client_config() -> None:
    config = SaturnConfig().load_object(default_client_config)
    assert config
