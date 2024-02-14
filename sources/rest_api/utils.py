from functools import reduce
from operator import getitem
from typing import Any, Dict


def join_url(base_url: str, path: str) -> str:
    if not base_url.endswith("/"):
        base_url += "/"
    return base_url + path.lstrip("/")


def create_nested_accessor(path):
    if isinstance(path, (list, tuple)):
        return lambda d: reduce(getitem, path, d)
    return lambda d: d.get(path)


def remove_key(d, key):
    return {k: v for k, v in d.items() if k != key}


def deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict:
    """Recursively merge b into a."""
    if isinstance(a, dict) and isinstance(b, dict):
        for key, value in b.items():
            if key in a:
                a[key] = deep_merge(a[key], value)
            else:
                a[key] = value
        return a
    return b