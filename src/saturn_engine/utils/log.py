from typing import Type
from typing import TypeVar
from typing import Union

import logging

T = TypeVar("T")


def getLogger(module: str, klass: Union[Type[T], T]) -> logging.Logger:
    if module.startswith("saturn_engine."):
        module = "saturn." + module[14:]
    if not isinstance(klass, type):
        klass = klass.__class__
    return logging.getLogger(f"{module}.{klass.__name__}")
