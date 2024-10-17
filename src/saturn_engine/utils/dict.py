import typing as t


def deep_merge(a: dict[str, t.Any], b: dict[str, t.Any]) -> dict[str, t.Any]:
    """
    Merge b into a
    """
    result = a.copy()
    for key, value in b.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result
