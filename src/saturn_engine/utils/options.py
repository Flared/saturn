import dataclasses
import functools
from abc import abstractmethod
from functools import cache
from typing import Any
from typing import Type
from typing import TypeVar

import desert
import marshmallow

T = TypeVar("T", bound="OptionsSchema")


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

    @classmethod
    def from_options(
        cls: Type[T], options_dict: dict, *args: object, **kwargs: object
    ) -> T:
        options_schema = cls.options_schema(unknown=marshmallow.EXCLUDE)
        options: OptionsSchema.Options = options_schema.load(options_dict)
        return cls(*args, options=options, **kwargs)


@cache
def schema_for(klass: Type[T], **meta: Any) -> marshmallow.Schema:
    return desert.schema(klass, meta=meta)
