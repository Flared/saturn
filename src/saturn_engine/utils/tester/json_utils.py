from typing import Iterator


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
