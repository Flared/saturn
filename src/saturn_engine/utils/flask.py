from typing import Generic
from typing import TypeVar
from typing import cast

import flask

from .options import asdict

T = TypeVar("T")


class Json(Generic[T]):
    pass


def jsonify(item: T) -> Json[T]:
    return cast(Json[T], flask.jsonify(asdict(item)))
