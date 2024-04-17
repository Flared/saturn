import typing as t

import dataclasses
import json
from abc import abstractmethod

import pydantic.v1
import pydantic.v1.json

from .cache import threadsafe_cache

OptionsSchemaT = t.TypeVar("OptionsSchemaT", bound="OptionsSchema")
T = t.TypeVar("T")


class OptionsSchema:
    @dataclasses.dataclass
    class Options:
        pass

    @abstractmethod
    def __init__(self, *args: object, options: Options, **kwargs: object) -> None: ...

    @classmethod
    def from_options(
        cls: t.Type[OptionsSchemaT], options_dict: dict, *args: object, **kwargs: object
    ) -> OptionsSchemaT:
        options: OptionsSchema.Options = fromdict(options_dict, cls.Options)
        return cls(*args, options=options, **kwargs)


class ModelConfig(pydantic.v1.BaseConfig):
    arbitrary_types_allowed = True


@threadsafe_cache
def schema_for(klass: t.Type) -> t.Type[pydantic.v1.BaseModel]:
    if issubclass(klass, pydantic.v1.BaseModel):
        return klass
    if dataclasses.is_dataclass(klass):
        return pydantic.v1.dataclasses.create_pydantic_model_from_dataclass(
            klass,  # type: ignore[arg-type]
            config=ModelConfig,
        )
    raise ValueError(f"Cannot get shema for {klass}")


def asdict(o: t.Any) -> dict[str, t.Any]:
    return pydantic.v1.json.pydantic_encoder(o)


def json_serializer(*args: t.Any, **kwargs: t.Any) -> str:
    return json.dumps(*args, default=pydantic.v1.json.pydantic_encoder, **kwargs)


def fromdict(
    d: dict[str, t.Any], klass: t.Type[T], *, config: t.Optional[dict] = None
) -> T:
    schema = schema_for(t.cast(t.Hashable, klass))
    obj: pydantic.v1.BaseModel = schema.parse_obj(d)
    if dataclasses.is_dataclass(klass):
        return t.cast(T, klass(**obj.dict()))
    return t.cast(T, obj)
