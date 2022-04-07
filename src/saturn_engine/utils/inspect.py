from typing import Any
from typing import Callable
from typing import Type
from typing import Union

import functools
import inspect
import sys
import threading
from collections.abc import Iterable


def eval_annotations(func: Callable, signature: inspect.Signature) -> inspect.Signature:
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
def eval_annotation(func: Callable, annotation: str) -> Type:
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
    klass: Type, annotations: Iterable[Union[Type, str]]
) -> list[Type]:
    class_globals = sys.modules[klass.__module__].__dict__
    eval_annotations = []
    for annotation in annotations:
        if isinstance(annotation, str):
            eval_annotations.append(eval(annotation, class_globals, None))  # noqa: S307
        else:
            eval_annotations.append(annotation)
    return eval_annotations


_import_lock = threading.Lock()


def synchronized_import(name: str, *, level: int) -> Any:
    """Import a module from name holding a lock.
    __import__ is not threadsafe since Python 3.3, having two thread importing
    module at once can (and has) led to circular import that would otherwise
    never happen on a single thread.
    This function is pretty much the old behaviour: Use a "global" lock.
    """
    with _import_lock:
        return __import__(name, level=level)


# Taken from CPython pickle.py
def get_import_name(obj: Callable) -> str:
    name = getattr(obj, "__qualname__", None)
    if name is None:
        name = obj.__name__

    module_name = whichmodule(obj, name)
    try:
        synchronized_import(module_name, level=0)
        module = sys.modules[module_name]
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


def import_name(name: str) -> Any:
    module, _, name = name.rpartition(".")
    while module:
        try:
            synchronized_import(module, level=0)
            return getattribute(sys.modules[module], name)[0]
        except ModuleNotFoundError:
            prev_name = name
            module, _, name = module.rpartition(".")
            name += "." + prev_name
    raise ModuleNotFoundError(name)


@functools.cache
def signature(func: Callable) -> inspect.Signature:
    _signature = inspect.signature(func)
    _signature = eval_annotations(func, _signature)
    return _signature
