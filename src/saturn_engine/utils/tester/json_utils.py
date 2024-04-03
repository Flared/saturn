from typing import Iterator
from typing import overload

import dataclasses


@overload
def normalize_json(obj: dict) -> dict: ...


@overload
def normalize_json(obj: list) -> list: ...


def normalize_json(obj: object) -> object:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        obj = dataclasses.asdict(obj)
    if isinstance(obj, dict):
        return {k: normalize_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [normalize_json(item) for item in obj]
    else:
        return obj


def find_nodes(obj: dict, val: object, path: list[str]) -> Iterator[list[str]]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if v == val:
                yield path + [k]
            else:
                yield from find_nodes(obj[k], val, path + [k])
    elif isinstance(obj, list):
        for idx, item in enumerate(obj):
            yield from find_nodes(item, val, path + [str(idx)])


def get_node_value(obj: dict, path: list[str]) -> object:
    cur_obj = obj
    for k in path[:-1]:
        if isinstance(cur_obj, list):
            cur_obj = cur_obj[int(k)]
        else:
            cur_obj = cur_obj[k]
    return cur_obj[path[-1]]


def replace_node(obj: dict, path: list[str], replace_value: object) -> None:
    cur_obj = obj
    for k in path[:-1]:
        if isinstance(cur_obj, list):
            cur_obj = cur_obj[int(k)]
        else:
            cur_obj = cur_obj[k]
    cur_obj[path[-1]] = replace_value


def replace_nodes_by_value(obj: dict, val: object, replace_value: object) -> None:
    for path in find_nodes(obj, val, []):
        replace_node(obj, path, replace_value)
