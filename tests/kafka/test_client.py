from typing import Dict, List, Tuple

import pytest

from sources.kafka.helpers.client import group_offsets

from tests.kafka.utils import message


@pytest.mark.parametrize(
    "groups,expected",
    [
        ([], []),
        ([[("t", 0, 0), ("t", 0, 1), ("t", 0, 2)]], [{("t", 0): 2}]),
        # the last read offset is stored although the last group ends earlier
        (
            [[("t", 0, 0), ("t", 0, 2), ("t", 0, 4)], [("t", 0, 1), ("t", 0, 3)]],
            [{("t", 0): 0}, {("t", 0): 4}],
        ),
        (
            [[("t", 0, 0), ("t", 1, 0)], [("t", 0, 1)], [("t", 0, 2), ("t", 1, 1)]],
            [{("t", 0): 0, ("t", 1): 0}, {("t", 0): 1}, {("t", 0): 2, ("t", 1): 1}],
        ),
        (
            [[("t", 0, 0), ("t", 0, 1)], [("t", 1, 5), ("t", 1, 6)]],
            [{("t", 0): 1}, {("t", 1): 6}],
        ),
        (
            [[("a", 0, 3)], [("b", 0, 7)], [("a", 0, 4)]],
            [{("a", 0): 3}, {("b", 0): 7}, {("a", 0): 4}],
        ),
    ],
    ids=[
        "no_groups",
        "one_group",
        "interleaved_groups",
        "partition_in_later_groups",
        "partition_per_group",
        "topic_per_group",
    ],
)
def test_group_offsets(
    groups: List[List[Tuple[str, int, int]]],
    expected: List[Dict[Tuple[str, int], int]],
) -> None:
    batch = [
        [message(topic, p, offset) for topic, p, offset in group] for group in groups
    ]
    assert group_offsets(batch) == expected
