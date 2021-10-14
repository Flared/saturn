import dataclasses
import functools
from collections.abc import AsyncGenerator
from typing import Any
from typing import Type
from typing import TypeVar

import desert
import marshmallow

from saturn_engine.core import Message

T = TypeVar("T")


class Queue:
    @dataclasses.dataclass
    class Options:
        pass

    async def run(self) -> AsyncGenerator[Message, None]:
        for _ in ():
            yield _

    @classmethod
    async def new(cls: Type[T], id: str, options: Any) -> T:
        raise NotImplementedError()

    @classmethod
    @functools.cache
    def options_schema(cls, **meta: Any) -> marshmallow.Schema:
        return desert.schema(cls.Options, meta=meta)
