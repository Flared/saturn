import collections
import inspect
import os
import typing
from collections.abc import Iterable
from collections.abc import Mapping
from enum import Enum
from types import GenericAlias
from typing import Any
from typing import Generic
from typing import Type
from typing import TypeVar
from typing import cast

from .inspect import eval_class_annotations
from .inspect import import_name

T = TypeVar("T")
U = TypeVar("U")

_MISSING = object()


class Namespace(collections.UserDict):
    def __getattr__(self, name: str) -> Any:
        return self.data[name.lower()]

    def __getitem__(self, name: str) -> Any:
        return self.data[name.lower()]

    def __setitem__(self, name: str, value: Any) -> None:
        self.data[name.lower()] = value


class Config(Generic[T]):
    def __init__(self, interface: Type[T]) -> None:
        self._layers: list[object] = []
        self._config: Namespace = Namespace()
        self._interfaces: dict[str, Any] = {}
        self._main_interface = interface

    def load_object(self, obj: Any) -> None:
        """Load an object into the configuration.

        This can be a module, class, instance or dict.
        """
        if obj is None:
            return

        if isinstance(obj, str):
            obj = import_name(obj)

        self._layers.append(obj)
        load_config(layers=[obj], interfaces=self._interfaces, config=self._config)

    def load_envvar(self, envvar: str) -> None:
        """Load an object from a path stored in an environment variable."""
        self.load_object(os.environ.get(envvar))

    def register_interface(self, namespace: str, interface: Type) -> None:
        """Add an interface to the config under namespace.

        Interfaces are used to validate config keys and values.
        """
        namespace = namespace.lower()
        existing = self._interfaces.get(namespace)
        if existing:
            raise ValueError(
                f"Interface already registered for {namespace}: {existing}"
            )
        self._interfaces[namespace] = interface
        load_config(
            layers=self._layers, interfaces={namespace: interface}, config=self._config
        )

    @property
    def c(self) -> T:
        if "" not in self._interfaces:
            self.register_interface("", self._main_interface)
        return cast(T, self._config)

    @property
    def r(self) -> Namespace:
        return self._config

    def cast_namespace(self, namespace: str, interface: Type[U]) -> U:
        namespace = namespace.lower()
        if namespace not in self._interfaces:
            self.register_interface(namespace, interface)
        return cast(U, self._config[namespace])


def load_config_interface(
    *, interface: Type, layers: list, config: Namespace, path: str = ""
) -> None:
    annotations = typing.get_type_hints(interface)
    for name in dir(interface) | annotations.keys():
        if name.startswith("_"):
            continue

        key_path = f"{path}.{name}" if path else name
        value = get_attr(interface, name)
        typ = get_prop_type(interface, name, value)

        if issubclass(typ, (str, int, float, list, dict, tuple, Enum)) or isinstance(
            typ, GenericAlias
        ):
            config_value = get_config_value(layers, name, value)
            if config_value is _MISSING:
                if name not in config:
                    raise ValueError(f"Missing config key {key_path}")
            elif not check_type(config_value, typ, interface):
                raise ValueError(
                    f"Invalid config key '{key_path}' type: "
                    f"expected '{desc_type(typ)}', "
                    f"got '{type(config_value).__name__}'"
                )
            config[name] = config_value

        elif inspect.isclass(typ):
            sublayers_ = sublayers(layers, name)
            load_config_interface(
                interface=typ,
                layers=sublayers_,
                config=config.setdefault(name, Namespace()),
                path=key_path,
            )


def get_config_value(layers: list, name: str, default_value: str) -> Any:
    config_value = _MISSING
    for source in reversed(layers):
        config_value = get_attr(source, name)
        if config_value is not _MISSING:
            break
    if config_value is _MISSING:
        config_value = default_value
    return config_value


def load_config(
    *, layers: list, interfaces: dict[str, Type], config: Namespace
) -> None:
    for namespace, interface in interfaces.items():
        if namespace:
            config = config.setdefault(namespace, Namespace())
            layers = sublayers(layers, namespace)
        load_config_interface(interface=interface, layers=layers, config=config)


def sublayers(layers: list, name: str) -> list:
    return [s for layer in layers if (s := get_attr(layer, name)) is not _MISSING]


def desc_type(typ: Any) -> str:
    if isinstance(typ, GenericAlias):
        return str(typ)
    return typ.__name__


def get_attr(obj: Any, name: str) -> Any:
    if isinstance(obj, dict):
        ikey_map = {k.lower(): k for k in obj}
        k = ikey_map.get(name, _MISSING)
        return obj.get(k, _MISSING)

    ikey_map = {k.lower(): k for k in dir(obj)}
    k = ikey_map.get(name, _MISSING)
    if k is _MISSING:
        return k
    return getattr(obj, k, _MISSING)


def get_prop_type(interface: Any, name: str, value: Any) -> Any:
    annotations = typing.get_type_hints(interface)
    annotation = annotations.get(name)

    if annotation:
        typ = annotation
    elif inspect.isclass(value):
        typ = value
    elif value not in (None, _MISSING):
        typ = type(value)
    else:
        typ = str
    return typ


def check_type(obj: Any, typ: Any, scope: Any) -> bool:
    if typ is typing.Any:
        return True

    typ_args: list[Type] = []
    if isinstance(typ, GenericAlias):
        ga_typ = typ
        typ = typing.get_origin(ga_typ)
        typ_args = eval_class_annotations(scope, typing.get_args(ga_typ))

    if not isinstance(obj, typ):
        return False

    if issubclass(typ, Iterable) and len(typ_args) == 1:
        return all(check_type(o, typ_args[0], scope) for o in obj)
    elif issubclass(typ, Mapping) and len(typ_args) == 2:
        return all(
            check_type(k, typ_args[0], scope) and check_type(v, typ_args[1], scope)
            for k, v in obj.items()
        )

    return True
