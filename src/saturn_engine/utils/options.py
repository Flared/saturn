from saturn_engine.utils.inspect import import_name
import typing as t

import dataclasses
import json
from abc import abstractmethod
from functools import cache

import pydantic
import pydantic.json

OptionsSchemaT = t.TypeVar("OptionsSchemaT", bound="OptionsSchema")
T = t.TypeVar("T")


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
def schema_for(klass: t.Type) -> t.Type[pydantic.BaseModel]:
    if issubclass(klass, pydantic.BaseModel):
        return klass
    if dataclasses.is_dataclass(klass):
        return pydantic.dataclasses.create_pydantic_model_from_dataclass(klass)  # type: ignore[arg-type]
    raise ValueError(f"Cannot get shema for {klass}")


def asdict(o: t.Any) -> dict[str, t.Any]:
    return pydantic.json.pydantic_encoder(o)


def json_serializer(*args: t.Any, **kwargs: t.Any) -> str:
    return json.dumps(*args, default=pydantic.json.pydantic_encoder, **kwargs)


def fromdict(
    d: dict[str, t.Any], klass: t.Type[T], *, config: t.Optional[dict] = None
) -> T:
    schema = schema_for(t.cast(t.Hashable, klass))
    obj: pydantic.BaseModel = schema.parse_obj(d)
    if dataclasses.is_dataclass(klass):
        return t.cast(T, klass(**obj.dict()))
    return t.cast(T, obj)

class SymbolName(t.Generic[T]):
    def __init__(self, obj: T) -> None:
        self.object = obj

    @classmethod
    def __get_validators__(self) -> t.Iterator[t.Callable]:
        yield self.load_symbol

    @classmethod
    def load_symbol(cls, v: object, field: pydantic.fields.ModelField) -> T:
        if isinstance(v, str):
            v = import_name(v)

        error = None
        if field.sub_fields:
            field = field.sub_fields[0]
            v, error = field.validate(v, {}, loc="type")

        if error:
            raise pydantic.ValidationError([error], cls)

        return cls(v)


