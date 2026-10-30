import pytest

from sources.mongodb.partition import _sample_size, and_filters

# splitting collections is tested on the local server in test_mongodb_source.py, mocks of its
# type bracketing and `$bucketAuto` would test our assumptions only


@pytest.mark.parametrize(
    "ranges,count,expected",
    [(10, 1_000_000, 100), (10, 1_000, 40), (10, 100, 11)],
    ids=["ten_per_range", "four_percent", "one_more_than_ranges"],
)
def test_sample_size(ranges: int, count: int, expected: int) -> None:
    assert _sample_size(ranges, count) == expected


def test_and_filters() -> None:
    assert and_filters({}, {}) == {}
    assert and_filters({"n": 1}, {}) == {"n": 1}
    assert and_filters({"n": 1}, {"_id": {"$lt": 2}}) == {
        "$and": [{"n": 1}, {"_id": {"$lt": 2}}]
    }
