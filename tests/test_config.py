import re

import pytest

from saturn_engine.config import Config as SaturnConfig
from saturn_engine.config import default_config
from saturn_engine.utils.config import Config


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
    assert config.r.D.Z == "6"
    assert config.r["d"]["Z"] == "6"

    d: ExtraInterface = config.cast_namespace("d", ExtraInterface)
    assert d.y == 2


def test_config_error() -> None:
    config: Config[Interface] = Config()

    with pytest.raises(AttributeError):
        config.c.a

    config = config.load_object(ObjectConfig).register_interface("", Interface)

    with pytest.raises(
        ValueError, match="Invalid config key 'a' type: expected 'str', got 'int'"
    ):
        config.load_object({"a": 1})

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Invalid config key 'c.y' type: expected 'dict[str, list[int]]', got 'dict'"
        ),
    ):
        config.load_object({"c": {"y": {"1": ["a"]}}})


def test_default_config() -> None:
    # Test default_config can be loaded by itself.
    config = SaturnConfig().load_object(default_config)
    assert config
    assert True
