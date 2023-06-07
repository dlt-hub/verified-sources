"""Pipedrive source helpers"""

from dlt.common import pendulum
from typing import Any, Iterable, Tuple, Dict, List, cast
from itertools import groupby


def parse_timestamp(timestamp: Any) -> str:
    if not timestamp:
        return None
    if isinstance(timestamp, str):
        timestamp = pendulum.parse(timestamp)
    assert isinstance(
        timestamp, pendulum.DateTime
    ), "since_timestamp must be a valid ISO datetime string or pendulum.DateTime object"
    timestamp = timestamp.in_timezone("UTC")
    return cast(str, timestamp.to_iso8601_string().replace("T", " ").replace("Z", ""))


def _deals_flow_group_key(item: Dict[str, Any]) -> str:
    return item["object"]  # type: ignore[no-any-return]


def group_deal_flows(
    pages: Iterable[Iterable[Dict[str, Any]]]
) -> Iterable[Tuple[str, List[Dict[str, Any]]]]:
    for page in pages:
        for entity, items in groupby(
            sorted(page, key=_deals_flow_group_key), key=_deals_flow_group_key
        ):
            yield entity, [
                dict(item["data"], timestamp=item["timestamp"]) for item in items
            ]
