import dlt

from github import github_reactions, github_repo_events


def load_duckdb_repo_reactions_issues_only() -> None:
    """Loads issues, their comments and reactions for duckdb"""
    pipeline = dlt.pipeline(
        "github_reactions",
        destination="duckdb",
        dataset_name="duckdb_issues",
        full_refresh=True,
    )
    # get only 100 items (for issues and pull request)
    data = github_reactions(
        "duckdb", "duckdb", items_per_page=100, max_items=100
    ).with_resources("issues")
    print(pipeline.run(data))


def load_airflow_events() -> None:
    """Loads airflow events. Shows incremental loading. Forces anonymous access token"""
    pipeline = dlt.pipeline(
        "github_events", destination="duckdb", dataset_name="airflow_events"
    )
    data = github_repo_events("apache", "airflow", access_token="")
    print(pipeline.run(data))
    # if you uncomment this, it does not load the same events again
    # data = github_repo_events("apache", "airflow", access_token="")
    # print(pipeline.run(data))


def load_dlthub_dlt_all_data() -> None:
    """Loads all issues, pull requests and comments for dlthub dlt repo"""
    pipeline = dlt.pipeline(
        "github_reactions",
        destination="duckdb",
        dataset_name="dlthub_reactions",
        full_refresh=True,
    )
    data = github_reactions("dlt-hub", "dlt")
    print(pipeline.run(data))


if __name__ == "__main__":
    load_duckdb_repo_reactions_issues_only()
    load_airflow_events()
    load_dlthub_dlt_all_data()
