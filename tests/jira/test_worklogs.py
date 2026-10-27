import dlt

from sources.jira import worklogs

from tests.jira.utils import CREDENTIALS, JiraMock, T1, make_worklog, to_rows
from tests.utils import assert_load_info, load_table_counts, load_tables_to_dicts


def test_worklogs(jira_mock: JiraMock) -> None:
    assert list(worklogs(**CREDENTIALS)) == []
    assert jira_mock.worklog_lists == []

    jira_mock.worklog_feeds.clear()
    jira_mock.worklogs = {
        str(worklog_id): make_worklog(worklog_id, 1, T1 + worklog_id * 60000)
        for worklog_id in (1, 2, 3)
    }
    jira_mock.worklog_page_size = 2
    pipeline = dlt.pipeline("jira_worklogs", destination="duckdb", dev_mode=True)
    assert_load_info(pipeline.run(worklogs(**CREDENTIALS)))

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
    assert columns["deleted"]["hard_delete"] is True

    # worklog 2 is deleted, the next run starts at the last update minus 10 minutes of lag
    jira_mock.worklog_feeds.clear()
    del jira_mock.worklogs["2"]
    jira_mock.deleted_worklogs = [{"worklogId": 2, "updatedTime": T1 + 4 * 60000}]
    assert_load_info(pipeline.run(worklogs(**CREDENTIALS)))
    since = T1 + 3 * 60000 - 600000
    assert jira_mock.worklog_feeds == [
        {"feed": "updated", "since": since},
        {"feed": "deleted", "since": since},
    ]
    rows = load_tables_to_dicts(pipeline, "worklogs")["worklogs"]
    assert sorted(row["id"] for row in rows) == ["1", "3"]

    # more worklogs than a list request takes
    jira_mock.worklogs = {
        str(worklog_id): make_worklog(worklog_id, 1, T1 + worklog_id)
        for worklog_id in range(1, 1002)
    }
    jira_mock.worklog_lists.clear()
    jira_mock.deleted_worklogs = []
    jira_mock.worklog_page_size = 2000
    assert len(to_rows(list(worklogs(**CREDENTIALS)))) == 1001
    assert [len(ids) for ids in jira_mock.worklog_lists] == [1000, 1]
