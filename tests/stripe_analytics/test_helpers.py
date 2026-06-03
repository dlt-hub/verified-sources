from typing import Any, Dict, List, Optional

import pytest
import stripe
from pendulum import datetime

from sources.stripe_analytics.helpers import (
    pagination,
    stripe_get_data,
    transform_date,
)


@pytest.mark.parametrize(
    "date, expected",
    [
        ("2024-01-01T00:00:00Z", 1704067200),
        (datetime(2024, 1, 1), 1704067200),
        (1704067200, 1704067200),
    ],
    ids=["iso_string", "pendulum_datetime", "unix_timestamp"],
)
def test_transform_date(date: Any, expected: int) -> None:
    assert transform_date(date) == expected


def make_list_object(ids: List[str], has_more: bool = False) -> stripe.ListObject:
    return stripe.ListObject.construct_from(
        {
            "object": "list",
            "data": [{"id": id_, "object": "customer"} for id_ in ids],
            "has_more": has_more,
        },
        "sk_test",
    )


def test_stripe_get_data_returns_plain_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: List[Dict[str, Any]] = []

    def mock_list(**kwargs: Any) -> stripe.ListObject:
        calls.append(kwargs)
        return make_list_object(["cus_1"])

    monkeypatch.setattr(stripe.Customer, "list", mock_list)

    response = stripe_get_data(
        "Customer", start_date=datetime(2024, 1, 1), end_date=1706745600
    )
    # response must be a plain dict regardless of installed stripe version
    assert isinstance(response, dict)
    assert response["has_more"] is False
    assert response["data"][0]["id"] == "cus_1"
    assert calls[0]["created"] == {"gte": 1704067200, "lt": 1706745600}
    assert calls[0]["limit"] == 100


def test_stripe_get_data_subscription_status(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: List[Dict[str, Any]] = []

    def mock_list(**kwargs: Any) -> stripe.ListObject:
        calls.append(kwargs)
        return make_list_object(["sub_1"])

    monkeypatch.setattr(stripe.Subscription, "list", mock_list)

    stripe_get_data("Subscription")
    # all subscriptions are requested, including canceled ones
    assert calls[0]["status"] == "all"


def test_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = [
        make_list_object(["cus_1", "cus_2"], has_more=True),
        make_list_object(["cus_3"], has_more=False),
    ]
    calls: List[Optional[str]] = []

    def mock_list(**kwargs: Any) -> stripe.ListObject:
        calls.append(kwargs.get("starting_after"))
        return pages[len(calls) - 1]

    monkeypatch.setattr(stripe.Customer, "list", mock_list)

    items = [item for page in pagination("Customer") for item in page]
    assert [item["id"] for item in items] == ["cus_1", "cus_2", "cus_3"]
    # the second page is requested starting after the last item of the first one
    assert calls == [None, "cus_2"]
