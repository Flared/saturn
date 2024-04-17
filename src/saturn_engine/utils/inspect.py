import typing as t

import dataclasses
import importlib
import inspect
import sys
import threading
from collections.abc import Iterable

import typing_inspect

from .cache import threadsafe_cache

R = t.TypeVar("R")


def eval_annotations(
    func: t.Callable, signature: inspect.Signature
) -> inspect.Signature:
    is_dirty = False
    evaluated_parameters: list[inspect.Parameter] = []
    for parameter in signature.parameters.values():
        if isinstance(parameter.annotation, str):
            is_dirty = True
            parameter = parameter.replace(
                annotation=eval_annotation(func, parameter.annotation)
            )
        evaluated_parameters.append(parameter)

    if is_dirty:
        signature = signature.replace(parameters=evaluated_parameters)

    if isinstance(signature.return_annotation, str):
        signature = signature.replace(
            return_annotation=eval_annotation(func, signature.return_annotation)
        )

    return signature


# Taken from cpython 3.10 inspect.get_annotations
def eval_annotation(func: t.Callable, annotation: str) -> t.Type:
    unwrap = func
    while True:
        if hasattr(unwrap, "__wrapped__"):
            unwrap = unwrap.__wrapped__  # type: ignore[attr-defined]
            continue
        break
    obj_globals = getattr(unwrap, "__globals__", None)
    # Scary eval! This is safe because only used on function object annotations.
    # If someone is able to build arbitrary function object, then it's able to
    # do much more than eval.
    return eval(annotation, obj_globals, None)  # noqa: S307


def eval_class_annotations(
    klass: t.Type, annotations: Iterable[t.Union[t.Type, str]]
) -> list[t.Type]:
    class_globals = sys.modules[klass.__module__].__dict__
    eval_annotations = []
    for annotation in annotations:
        if isinstance(annotation, str):
            eval_annotations.append(eval(annotation, class_globals, None))  # noqa: S307
        else:
            eval_annotations.append(annotation)
    return eval_annotations


_import_lock = threading.Lock()


# Taken from CPython pickle.py
def get_import_name(obj: t.Callable) -> str:
    name = getattr(obj, "__qualname__", None)
    if name is None:
        name = obj.__name__

    module_name = whichmodule(obj, name)
    try:
        module = importlib.import_module(module_name)
        obj2, parent = getattribute(module, name)
    except (ImportError, KeyError, AttributeError):
        raise ValueError(
            "Can't get %r info: it's not found as %s.%s" % (obj, module_name, name)
        ) from None
    else:
        if obj2 is not obj:
            raise ValueError(
                "Can't get %r info: it's not the same object as %s.%s"
                % (obj, module_name, name)
            )

    return f"{module_name}.{name}"


def getattribute(obj: object, name: str) -> tuple[object, object]:
    for subpath in name.split("."):
        if subpath == "<locals>":
            raise AttributeError(
                "Can't get local attribute {!r} on {!r}".format(name, obj)
            )
        try:
            parent = obj
            obj = getattr(obj, subpath)
        except AttributeError:
            raise AttributeError(
                "Can't get attribute {!r} on {!r}".format(name, obj)
            ) from None
    return obj, parent


def whichmodule(obj: object, name: str) -> str:
    """Find the module an object belong to."""
    module_name = getattr(obj, "__module__", None)
    if module_name is not None:
        return module_name
    # Protect the iteration by using a list copy of sys.modules against dynamic
    # modules that trigger imports of other modules upon calls to getattr.
    for module_name, module in sys.modules.copy().items():
        if (
            module_name == "__main__"
            or module_name == "__mp_main__"  # bpo-42406
            or module is None
        ):
            continue
        try:
            if getattribute(module, name)[0] is obj:
                return module_name
        except AttributeError:
            pass
    return "__main__"


def import_name(name: str) -> t.Any:
    module, _, name = name.rpartition(".")
    while module:
        try:
            mod = importlib.import_module(module)
            return getattribute(mod, name)[0]
        except ModuleNotFoundError:
            prev_name = name
            module, _, name = module.rpartition(".")
            name += "." + prev_name
    raise ModuleNotFoundError(name)


@threadsafe_cache
def signature(func: t.Callable) -> inspect.Signature:
    _signature = inspect.signature(func)
    _signature = eval_annotations(func, _signature)
    return _signature


def unwrap_optional(typ: t.Any) -> t.Optional[t.Type]:
    if typing_inspect.is_optional_type(typ):
        args = typing_inspect.get_args(typ)
        if args:
            return args[0]
    return None


class BaseParamsDataclass(t.Generic[R]):
    _func: t.ClassVar[t.Callable]
    _has_kwargs: t.ClassVar[bool]

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        pass

    @classmethod
    def find_by_type(cls, typ: t.Type) -> t.Optional[str]:
        for field in dataclasses.fields(cls):  # type: ignore[arg-type]
            field_type = field.type
            if inner_typ := unwrap_optional(field_type):
                field_type = inner_typ

            if isinstance(field_type, type) and issubclass(field_type, typ):
                return field.name

            origin = t.get_origin(field_type)
            if origin is t.Annotated:
                if any(issubclass(t, typ) for t in t.get_args(field_type)[1:]):
                    return field.name
        return None

    def call(self, *, kwargs: t.Optional[dict[str, t.Any]] = None) -> R:
        args_dict = self.__dict__.copy()
        if kwargs is None or not self._has_kwargs:
            kwargs = {}
        if self._has_kwargs:
            kwargs = {k: v for k, v in kwargs.items() if k not in args_dict}
        return self._func(**args_dict, **kwargs)


@threadsafe_cache
def dataclass_from_params(func: t.Callable[..., R]) -> t.Type[BaseParamsDataclass[R]]:
    cls_name = func.__name__ + ".params"
    fields: list[tuple] = []
    func_signature = signature(func)
    namespace = {"_func": staticmethod(func), "_has_kwargs": False}
    for parameter in func_signature.parameters.values():
        name = parameter.name
        typ = parameter.annotation
        if typ is inspect.Parameter.empty:
            typ = t.Any

        field_extra = {}
        if parameter.default is not inspect.Parameter.empty:
            field_extra["default"] = parameter.default
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY:
            field_extra["kw_only"] = True
        elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
            namespace["_has_kwargs"] = True
            continue
        elif parameter.kind is not inspect.Parameter.POSITIONAL_OR_KEYWORD:
            continue

        field = [name, typ]
        if field_extra:
            field.append(dataclasses.field(**field_extra))

        fields.append(tuple(field))

    return t.cast(
        t.Type[BaseParamsDataclass[R]],
        dataclasses.make_dataclass(
            cls_name,
            fields,  # type: ignore[arg-type]
            bases=(BaseParamsDataclass,),
            namespace=namespace,
        ),
    )
