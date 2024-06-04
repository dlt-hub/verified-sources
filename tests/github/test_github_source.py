import pytest

import dlt

from sources.github import github_reactions, github_repo_events, github_stargazers

from tests.utils import (
    ALL_DESTINATIONS,
    assert_load_info,
    assert_query_data,
    load_table_counts,
    load_table_distinct_counts,
)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_github_reactions(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        "github_reactions",
        destination=destination_name,
        dataset_name="duckdb_issues",
        full_refresh=True,
    )
    # get only 100 items (for issues and pull request)
    data = github_reactions("duckdb", "duckdb", items_per_page=100, max_items=100)
    load_info = pipeline.run(data)
    assert_load_info(load_info)
    # assert tables created
    expected_tables = [
        "issues",
        "pull_requests",
        "issues__reactions",
        "pull_requests__reactions",
        "issues__comments",
        "pull_requests__comments",
        "pull_requests__comments__reactions",
        "issues__comments__reactions",
    ]
    # only those tables in the schema
    assert set(t["name"] for t in pipeline.default_schema.data_tables()) == set(
        expected_tables
    )
    # get counts
    table_counts = load_table_counts(pipeline, *expected_tables)
    # all tables loaded
    assert set(table_counts.keys()) == set(expected_tables)
    assert all(c > 0 for c in table_counts.values())
    assert table_counts["issues"] == 100
    assert table_counts["pull_requests"] == 100


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_github_events(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        "github_events",
        destination=destination_name,
        dataset_name="airflow_events",
        full_refresh=True,
    )
    data = github_repo_events("apache", "airflow")
    load_info = pipeline.run(data)
    assert_load_info(load_info)
    # all tables must end with event (auto created from event types) or contain "_event__" (child tables)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    assert all(
        name.endswith("_event") or name.find("_event__") != -1 for name in table_names
    )
    load_table_counts(pipeline, *table_names)
    # load again. it could happen that a single event got loaded but surely numbers should not double and events must be unique
    data = github_repo_events("apache", "airflow")
    pipeline.run(data)
    table_names = [
        t["name"]
        for t in pipeline.default_schema.data_tables()
        if t["name"].endswith("_event")
    ]
    table_counts = load_table_counts(pipeline, *table_names)
    table_distinct_id_counts = load_table_distinct_counts(pipeline, "id", *table_names)
    # no duplicates
    assert table_counts == table_distinct_id_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_github_stargazers(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        "github_stargazers",
        destination=destination_name,
        dataset_name="duckdb_stargazers",
        full_refresh=True,
    )
    # get only 100 stargazers
    data = github_stargazers("duckdb", "duckdb", items_per_page=100, max_items=100)
    load_info = pipeline.run(data)
    assert_load_info(load_info)
    # assert tables created
    expected_tables = ["stargazers"]
    # only those tables in the schema
    assert set(t["name"] for t in pipeline.default_schema.data_tables()) == set(
        expected_tables
    )
    # get counts
    table_counts = load_table_counts(pipeline, *expected_tables)
    # all tables loaded
    assert set(table_counts.keys()) == set(expected_tables)
    assert all(c > 0 for c in table_counts.values())
    assert table_counts["stargazers"] == 100
