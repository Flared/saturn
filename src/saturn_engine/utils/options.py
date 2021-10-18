import dataclasses
import functools
from abc import abstractmethod
from typing import Any

import desert
import marshmallow


class OptionsSchema:
    @dataclasses.dataclass
    class Options:
        pass

    @abstractmethod
    def __init__(self, *args: object, options: Options, **kwargs: object) -> None:
        ...

    @classmethod
    @functools.cache
    def options_schema(cls, **meta: Any) -> marshmallow.Schema:
        return desert.schema(cls.Options, meta=meta)
