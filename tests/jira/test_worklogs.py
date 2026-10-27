import dlt

from sources.jira import worklogs

from tests.jira.utils import (
    CREDENTIALS,
    JiraMock,
    T1,
    duckdb_pipeline,
    load,
    make_worklog,
    rows_by_id,
)
from tests.utils import load_table_counts


def test_worklogs(jira_mock: JiraMock) -> None:
    assert list(worklogs(**CREDENTIALS)) == []
    assert jira_mock.worklog_lists == []

    jira_mock.worklog_feeds.clear()
    jira_mock.worklogs = {
        str(worklog_id): make_worklog(worklog_id, 1, T1 + worklog_id * 60000)
        for worklog_id in (1, 2, 3)
    }
    jira_mock.worklog_page_size = 2
    pipeline = duckdb_pipeline("jira_worklogs")
    load(pipeline, worklogs(**CREDENTIALS))

    # the next page starts at the youngest worklog of the previous page
    assert jira_mock.worklog_feeds == [
        {"feed": "updated", "since": 0},
        {"feed": "updated", "since": T1 + 2 * 60000},
        {"feed": "deleted", "since": 0},
    ]
    assert jira_mock.worklog_lists == [[1, 2], [2, 3]]
    assert load_table_counts(pipeline, "worklogs") == {"worklogs": 3}
    columns = pipeline.default_schema.get_table_columns("worklogs")
    assert columns["updated"]["data_type"] == "timestamp"
    assert columns["updated"]["dedup_sort"] == "desc"
    assert columns["time_spent_seconds"]["data_type"] == "bigint"

    # worklog 2 is deleted, the next run starts at the last update minus 10 minutes of lag
    jira_mock.worklog_feeds.clear()
    del jira_mock.worklogs["2"]
    jira_mock.deleted_worklogs = [{"worklogId": 2, "updatedTime": T1 + 4 * 60000}]
    load(pipeline, worklogs(**CREDENTIALS))
    since = T1 + 3 * 60000 - 600000
    assert jira_mock.worklog_feeds == [
        {"feed": "updated", "since": since},
        {"feed": "deleted", "since": since},
    ]
    assert [row["id"] for row in rows_by_id(pipeline, "worklogs")] == ["1", "3"]
    # the column exists once a worklog was deleted, with the hint that removes the row
    columns = pipeline.default_schema.get_table_columns("worklogs")
    assert columns["deleted"]["hard_delete"] is True

    # nothing changed and no lag: the feeds list the last change once more, the incremental
    # records it and the next run extracts nothing
    no_lag = dlt.sources.incremental("updated", lag=0)
    pipeline.run(worklogs(**CREDENTIALS, updated=no_lag))
    assert pipeline.last_trace.last_normalize_info.row_counts["worklogs"] == 1
    pipeline.run(worklogs(**CREDENTIALS, updated=no_lag))
    assert "worklogs" not in pipeline.last_trace.last_normalize_info.row_counts
    assert [row["id"] for row in rows_by_id(pipeline, "worklogs")] == ["1", "3"]

    # more worklogs than a list request takes
    jira_mock.worklogs = {
        str(worklog_id): make_worklog(worklog_id, 1, T1 + worklog_id)
        for worklog_id in range(1, 1002)
    }
    jira_mock.worklog_lists.clear()
    jira_mock.deleted_worklogs = []
    jira_mock.worklog_page_size = 2000
    assert len(list(worklogs(**CREDENTIALS))) == 1001
    assert [len(ids) for ids in jira_mock.worklog_lists] == [1000, 1]
