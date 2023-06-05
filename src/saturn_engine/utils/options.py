import typing as t

import dataclasses
import json
from abc import abstractmethod
from functools import cache

import pydantic
import pydantic.json

OptionsSchemaT = t.TypeVar("OptionsSchemaT", bound="OptionsSchema")
T = t.TypeVar("T")


class SchemaModel(pydantic.BaseModel, t.Generic[T]):
    @classmethod
    def parse_obj(cls, /, obj: dict[str, t.Any]) -> T:  # type: ignore
        ...


class OptionsSchema:
    @dataclasses.dataclass
    class Options:
        pass

    @abstractmethod
    def __init__(self, *args: object, options: Options, **kwargs: object) -> None:
        ...

    @classmethod
    def from_options(
        cls: t.Type[OptionsSchemaT], options_dict: dict, *args: object, **kwargs: object
    ) -> OptionsSchemaT:
        options: OptionsSchema.Options = fromdict(options_dict, cls.Options)
        return cls(*args, options=options, **kwargs)


@cache
def schema_for(klass: t.Type[T]) -> t.Type[SchemaModel[T]]:
    if issubclass(klass, pydantic.BaseModel):
        return klass  # type: ignore[return-value]
    if pydantic.dataclasses.is_builtin_dataclass(klass):
        return pydantic.dataclasses.dataclass(klass)  # type: ignore[return-value]
    if dataclasses.is_dataclass(klass):
        return klass  # type: ignore[return-value]
    raise ValueError(f"Cannot get shema for {klass}")


def asdict(o: t.Any) -> dict[str, t.Any]:
    return pydantic.json.pydantic_encoder(o)


def json_serializer(*args: t.Any, **kwargs: t.Any) -> str:
    return json.dumps(*args, default=pydantic.json.pydantic_encoder, **kwargs)


def fromdict(
    d: dict[str, t.Any], klass: t.Type[T], *, config: t.Optional[dict] = None
) -> T:
    return schema_for(t.cast(t.Hashable, klass))(**d)  # type: ignore[return-value]
