import typing as t

def is_optional_type(typ: t.Any) -> bool: ...
def is_typevar(typ: t.Any) -> bool: ...
def get_args(typ: t.Any) -> tuple: ...
