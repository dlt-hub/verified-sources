from typing import Any, Dict, List


def unpack_factory_args(factory_args: Dict[str, Any]) -> List[Any]:
    """Unpacks filesystem factory arguments from pytest parameters."""
    return [factory_args.get(k) for k in ("bucket_url", "kwargs")]
