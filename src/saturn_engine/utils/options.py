from typing import Any
from typing import Optional
from typing import Type
from typing import TypeVar
from typing import cast

import dataclasses
import functools
from abc import abstractmethod
from collections.abc import Hashable
from collections.abc import Mapping
from functools import cache

import desert
import marshmallow

OptionsSchemaT = TypeVar("OptionsSchemaT", bound="OptionsSchema")
T = TypeVar("T")


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
        cls: Type[OptionsSchemaT], options_dict: dict, *args: object, **kwargs: object
    ) -> OptionsSchemaT:
        options_schema = cls.options_schema(unknown=marshmallow.EXCLUDE)
        options: OptionsSchema.Options = options_schema.load(options_dict)
        return cls(*args, options=options, **kwargs)


@cache
def schema_for(klass: Type[T], **meta: Any) -> marshmallow.Schema:
    return desert.schema(klass, meta=meta)


def asdict(o: Any) -> dict[str, Any]:
    return schema_for(o.__class__).dump(o)


def fromdict(d: dict[str, Any], klass: Type[T], **meta: Any) -> T:
    return schema_for(cast(Hashable, klass), **meta).load(d)


class ObjectUnion(marshmallow.fields.Field):
    def __init__(self, *args: Any, union: dict[str, Type], **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.class_to_name = {v: k for k, v in union.items()}
        self.name_to_schemas = {
            name: schema_for(cast(Hashable, klass)) for name, klass in union.items()
        }

    def _serialize(self, value: Any, attr: str, obj: Any, **kwargs: Any) -> dict:
        klass = value.__class__
        name = self.class_to_name.get(klass)
        if name is None:
            raise marshmallow.ValidationError(
                f"{klass.__name__} is not part of the union"
            )
        return {name: self.name_to_schemas[name].dump(value)}

    def _deserialize(
        self,
        value: dict,
        attr: Optional[str],
        data: Optional[Mapping[str, Any]],
        **kwargs: Any,
    ) -> Any:
        keys = value.keys()
        if len(keys) != 1:
            raise marshmallow.ValidationError(
                f"Union must have a single value: {len(keys)} found"
            )
        name = next(iter(keys))
        schema = self.name_to_schemas.get(name)
        if schema is None:
            raise marshmallow.ValidationError(f"{name} is not part of the union")
        return schema.load(value[name])


# Fix typing issue with desert.field
def field(marshmallow_field: marshmallow.fields.Field, **kwargs: Any) -> Any:
    return desert.field(marshmallow_field, **kwargs)
