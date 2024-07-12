"""Tests for the destinations alltogether."""

import os
import pytest

import dlt
from dlt.destinations import duckdb
from tests.utils import ALL_DESTINATIONS, TEST_STORAGE_ROOT


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_destination_info(destination_name):
    p = dlt.pipeline(
        pipeline_name="dest_info",
        destination=destination_name,
        dataset_name="dest_info_data",
    )
    assert p.destination.destination_info == {
        "destination_name": p.destination.destination_name,
        "destination_type": p.destination.destination_type,
        "environment": None,
        "fingerprint": p.destination.configuration(None).fingerprint(),
        "repr": p.destination.destination_description,
    }


def test_destination_info_environment():
    duck = duckdb(
        credentials=os.path.join(TEST_STORAGE_ROOT, "quack.duckdb"),
        destination_name="duck1",
        environment="production",
    )
    assert duck.destination_info == {
        "destination_name": "duck1",
        "destination_type": "dlt.destinations.duckdb",
        "environment": "production",
        "fingerprint": duck.configuration(None).fingerprint(),
        "repr": "duck1(dlt.destinations.duckdb)",
    }
