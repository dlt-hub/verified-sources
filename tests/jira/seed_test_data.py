"""Seeds the Jira test site with cases for online tests of the `jira` source.

The cases cover API limits and behaviors tested by other Jira connectors: more issues than a bulk
fetch batch, more changes and worklogs than embedded in an issue, deleted worklogs, status
transitions, all field types, empty fields, attachments, links, subtasks, edited, restricted and
deleted comments and many comments.

Issues of a case are labeled with `dlt-seed` and `dlt-seed-<case>`. A case is skipped when issues
with its label exist. Run from the `sources` folder to read credentials from `sources/.dlt`:

    cd sources
    uv run python ../tests/jira/seed_test_data.py            # shows missing cases
    uv run python ../tests/jira/seed_test_data.py --apply    # creates missing cases
    uv run python ../tests/jira/seed_test_data.py --delete   # deletes all seeded issues
    uv run python ../tests/jira/seed_test_data.py --capture  # captures responses and the manifest

The counts and values the end-to-end tests expect come from the constants below and from the
manifest of seeded issues that `--capture` records, so run it again after seeding.
"""

import argparse
import json
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib.parse import quote, urlparse

import dlt
from dlt.sources.helpers.requests import HTTPError
from dlt.sources.helpers.rest_client.typing import HTTPMethod

from sources.jira.helpers.client import HOOKS, JiraClient
from sources.jira.helpers.paginators import response_body
from sources.jira.settings import MAX_SEARCH_PAGE_SIZE

from tests.jira.utils import CAPTURED_RESPONSES, SEED_LABEL, load_captured

API = "rest/api/2/"
BULK_ISSUES = 250
CHANGELOG_EDITS = 120
WORKLOGS = 25
COMMENTS = 120
# issues each case creates, expected by the end-to-end tests
ISSUES_PER_CASE = {
    "bulk_issues": BULK_ISSUES,
    "long_changelog": 1,
    "status_transitions": 1,
    "many_worklogs": 1,
    "deleted_worklog": 1,
    "all_field_types": 1,
    "minimal_issue": 1,
    "attachment": 1,
    "links_and_subtasks": 4,
    "comments": 1,
    "many_comments": 1,
    "comment_changes": 1,
}
# the restricted comment is skipped on the test site, the deleted one is gone
COMMENTS_PER_CASE = {"comments": 2, "many_comments": COMMENTS}
# the deleted worklog is listed by the deleted feed only
WORKLOGS_PER_CASE = {"many_worklogs": WORKLOGS, "deleted_worklog": 1}
# an edit is a change, creating the issue is not
CHANGELOGS_PER_CASE = {"long_changelog": CHANGELOG_EDITS}
SUMMARIES_PER_CASE = {
    "minimal_issue": f"[{SEED_LABEL}] minimal issue",
    "long_changelog": f"[{SEED_LABEL}] long changelog, edit {CHANGELOG_EDITS}",
    "attachment": f"[{SEED_LABEL}] attachment",
}
ATTACHMENT_FILENAME = "dlt-seed.csv"
# seeded cases and custom field types with nested values captured for offline tests
CAPTURED_CASES = ("all-field-types", "attachment", "links-and-subtasks")
CAPTURED_COMMENTS_CASE = "comments"
CAPTURED_CUSTOM_TYPES = (
    "gh-sprint",
    "cascadingselect",
    "multiselect",
    "multicheckboxes",
    "multiuserpicker",
)
# custom field types that cannot be set with a plain value
UNSUPPORTED_CUSTOM_TYPES = (
    "gh-sprint",
    "gh-epic-link",
    "gh-lexo-rank",
    "atlassian-team",
)


def request(client: JiraClient, method: HTTPMethod, path: str, **kwargs: Any) -> Any:
    """Calls API v2 at `path`, raises errors with the messages returned by Jira"""
    response = client.rest_client.request(
        API + path, method=method, hooks=HOOKS, **kwargs
    )
    return response_body(response) if response.content else None


def issue_fields(
    project: str, case: str, summary: str, **fields: Any
) -> Dict[str, Any]:
    return {
        "project": {"key": project},
        "issuetype": {"name": "Task"},
        "summary": f"[{SEED_LABEL}] {summary}",
        "labels": [SEED_LABEL, f"{SEED_LABEL}-{case}"],
        **fields,
    }


def create_issue(
    client: JiraClient, project: str, case: str, summary: str, **fields: Any
) -> str:
    body = {"fields": issue_fields(project, case, summary, **fields)}
    key: str = request(client, "POST", "issue", json=body)["key"]
    print(f"  created {key}: {summary}")
    return key


def search_ids(client: JiraClient, jql: str) -> List[str]:
    return [
        issue["id"]
        for page in client.search(jql, "id", MAX_SEARCH_PAGE_SIZE)
        for issue in page
    ]


def case_label(case: str) -> str:
    return f"{SEED_LABEL}-{case.replace('_', '-')}"


def seeded_issues(exclude_cases: Iterable[str] = ()) -> List[Dict[str, Any]]:
    """Seeded issues as captured by `--capture`: id, key, labels, created and updated"""
    excluded = {case_label(case) for case in exclude_cases}
    return [
        issue
        for issue in load_captured()["seed"]
        if not excluded & set(issue["labels"])
    ]


def issues_of_case(seeded: List[Dict[str, Any]], case: str) -> List[Dict[str, Any]]:
    return [issue for issue in seeded if case_label(case) in issue["labels"]]


def expected_issues(exclude_cases: Iterable[str] = ()) -> int:
    """Number of seeded issues, from what the cases create"""
    excluded = set(exclude_cases)
    return sum(count for case, count in ISSUES_PER_CASE.items() if case not in excluded)


def subtask_type(client: JiraClient, project: str) -> str:
    issue_types = request(client, "GET", f"project/{project}")["issueTypes"]
    name: str = next(t["name"] for t in issue_types if t.get("subtask"))
    return name


def bulk_issues(client: JiraClient, project: str, case: str) -> None:
    """More issues than a bulk fetch batch of 100, created within the same minutes"""
    for start in range(0, BULK_ISSUES, 50):
        updates = [
            {"fields": issue_fields(project, case, f"bulk issue {n + 1}")}
            for n in range(start, min(start + 50, BULK_ISSUES))
        ]
        result = request(client, "POST", "issue/bulk", json={"issueUpdates": updates})
        print(
            f"  created {len(result['issues'])} issues, errors: {result.get('errors')}"
        )


def long_changelog(client: JiraClient, project: str, case: str) -> None:
    """More changes than the changelog embedded in an issue and than a changelog page"""
    key = create_issue(client, project, case, "long changelog")
    for n in range(CHANGELOG_EDITS):
        summary = f"[{SEED_LABEL}] long changelog, edit {n + 1}"
        request(client, "PUT", f"issue/{key}", json={"fields": {"summary": summary}})
    print(f"  edited {key} {CHANGELOG_EDITS} times")


def status_transitions(client: JiraClient, project: str, case: str) -> None:
    """Status changes in the changelog, walks the available transitions"""
    key = create_issue(client, project, case, "status transitions")
    for n in range(6):
        transitions = request(client, "GET", f"issue/{key}/transitions")["transitions"]
        # rotate through transitions, some of them may require a screen and fail
        for transition in transitions[n % len(transitions) :] + transitions:
            try:
                request(
                    client,
                    "POST",
                    f"issue/{key}/transitions",
                    json={"transition": {"id": transition["id"]}},
                )
                print(f"  {key} -> {transition['to']['name']}")
                break
            except HTTPError:
                continue


def many_worklogs(client: JiraClient, project: str, case: str) -> None:
    """More worklogs than the 20 embedded in an issue, started in different time zones"""
    key = create_issue(client, project, case, "many worklogs")
    offsets = ["+0000", "+0530", "-0800", "+0100"]
    for n in range(WORKLOGS):
        worklog = {
            "started": f"2026-01-{n + 1:02d}T09:00:00.000{offsets[n % len(offsets)]}",
            "timeSpentSeconds": 900 * (n + 1),
            "comment": f"worklog {n + 1}: ünïcode ✓",
        }
        request(client, "POST", f"issue/{key}/worklog", json=worklog)
    print(f"  logged work {WORKLOGS} times on {key}")


def deleted_worklog(client: JiraClient, project: str, case: str) -> None:
    """A deleted worklog, listed by the deleted worklogs feed"""
    key = create_issue(client, project, case, "deleted worklog")
    started = "2026-01-01T09:00:00.000+0000"
    for comment in ("kept", "deleted"):
        worklog = request(
            client,
            "POST",
            f"issue/{key}/worklog",
            json={"started": started, "timeSpentSeconds": 600, "comment": comment},
        )
    request(client, "DELETE", f"issue/{key}/worklog/{worklog['id']}")
    print(f"  deleted worklog {worklog['id']} of {key}")


def comments(client: JiraClient, project: str, case: str) -> None:
    """Comments with wiki markup and unicode, an edited, a restricted and a deleted comment"""
    key = create_issue(client, project, case, "comments")
    path = f"issue/{key}/comment"
    body = "h2. Seeded\n*bold* comment with emoji 🚀, ünïcode and a\nnew line"
    request(client, "POST", path, json={"body": body})
    edited = request(client, "POST", path, json={"body": "before edit"})
    request(client, "PUT", f"{path}/{edited['id']}", json={"body": "after edit"})
    # requires the user to have the role, group visibility may be disabled on the site
    visibility = {"type": "role", "value": "Administrators"}
    try:
        request(
            client, "POST", path, json={"body": "restricted", "visibility": visibility}
        )
    except HTTPError as http_ex:
        print(f"  restricted comment skipped: {http_ex}")
    deleted = request(client, "POST", path, json={"body": "deleted"})
    request(client, "DELETE", f"{path}/{deleted['id']}")
    print(f"  commented {key}")


def many_comments(client: JiraClient, project: str, case: str) -> None:
    """More comments than a page of comments embedded in an issue"""
    key = create_issue(client, project, case, "many comments")
    for n in range(COMMENTS):
        request(
            client, "POST", f"issue/{key}/comment", json={"body": f"comment {n + 1}"}
        )
    print(f"  commented {key} {COMMENTS} times")


def comment_changes(client: JiraClient, project: str, case: str) -> None:
    """An issue on which online tests add, edit and delete their comments"""
    create_issue(client, project, case, "comment changes")


def field_value(field: Dict[str, Any], account_id: str) -> Any:
    """Returns a value for a field from create metadata, None if not supported"""
    schema = field.get("schema") or {}
    custom_type = (schema.get("custom") or "").split(":")[-1]
    allowed = field.get("allowedValues") or []
    if custom_type in UNSUPPORTED_CUSTOM_TYPES:
        return None
    if custom_type == "url":
        return "https://dlthub.com"
    if schema.get("type") == "option-with-child" and allowed:
        children = allowed[0].get("children") or []
        child = {"child": {"id": children[0]["id"]}} if children else {}
        return {"id": allowed[0]["id"], **child}
    if schema.get("type") == "array":
        if allowed:
            return [{"id": value["id"]} for value in allowed[:2]]
        if schema.get("items") == "user":
            return [{"accountId": account_id}]
        if schema.get("items") == "string":
            return ["custom-label", "ünïcode-label"]
        return None
    if allowed:
        return {"id": allowed[-1]["id"]}
    return {
        "string": "h1. Seeded\n*bold* text with emoji 🚀, ünïcode and a\nnew line",
        "number": 3.5,
        "date": "2026-02-01",
        "datetime": "2026-02-01T10:30:00.000+0530",
        "user": {"accountId": account_id},
        "timetracking": {"originalEstimate": "2h", "remainingEstimate": "1h 30m"},
    }.get(schema.get("type"))


def all_field_types(client: JiraClient, project: str, case: str) -> None:
    """Sets every field that can be set on create: custom field types and system fields"""
    account_id: str = request(client, "GET", "myself")["accountId"]
    issue_types = request(client, "GET", f"issue/createmeta/{project}/issuetypes")
    task_type = next(t for t in issue_types["issueTypes"] if t["name"] == "Task")
    meta = request(
        client, "GET", f"issue/createmeta/{project}/issuetypes/{task_type['id']}"
    )
    skip = {
        "project",
        "issuetype",
        "summary",
        "labels",
        "reporter",
        "parent",
        "issuelinks",
    }
    fields: Dict[str, Any] = {}
    for field in meta["fields"]:
        value = field_value(field, account_id)
        if field["fieldId"] not in skip and value is not None:
            fields[field["fieldId"]] = value
    try:
        create_issue(client, project, case, "all field types", **fields)
    except HTTPError as http_ex:
        # drop fields rejected by Jira and try again
        errors = http_ex.response.json().get("errors") or {}
        print(f"  dropping rejected fields: {errors}")
        create_issue(
            client,
            project,
            case,
            "all field types",
            **{k: v for k, v in fields.items() if k not in errors},
        )


def minimal_issue(client: JiraClient, project: str, case: str) -> None:
    """An issue with required fields only, all other fields are empty"""
    create_issue(client, project, case, "minimal issue")


def attachment(client: JiraClient, project: str, case: str) -> None:
    """An issue with an attachment"""
    key = create_issue(client, project, case, "attachment")
    # a multipart upload, `RESTClient.request` does not take `files`
    client.rest_client.session.post(
        f"{client.base_url}{API}issue/{key}/attachments",
        auth=client.auth,
        headers={"X-Atlassian-Token": "no-check"},
        files={"file": ("dlt-seed.csv", "id,name\n1,ünïcode\n".encode(), "text/csv")},
        hooks=HOOKS,
    )
    print(f"  attached a file to {key}")


def links_and_subtasks(client: JiraClient, project: str, case: str) -> None:
    """Subtasks of a parent and issue links in both directions"""
    parent = create_issue(client, project, case, "links parent")
    subtask = subtask_type(client, project)
    for n in (1, 2):
        create_issue(
            client,
            project,
            case,
            f"subtask {n}",
            issuetype={"name": subtask},
            parent={"key": parent},
        )
    other = create_issue(client, project, case, "links other")
    for link_type in ("Blocks", "Relates"):
        link = {
            "type": {"name": link_type},
            "inwardIssue": {"key": parent},
            "outwardIssue": {"key": other},
        }
        request(client, "POST", "issueLink", json=link)
    print(f"  linked {parent} and {other}")


CASES: Dict[str, Callable[[JiraClient, str, str], None]] = {
    "bulk_issues": bulk_issues,
    "long_changelog": long_changelog,
    "status_transitions": status_transitions,
    "many_worklogs": many_worklogs,
    "deleted_worklog": deleted_worklog,
    "all_field_types": all_field_types,
    "minimal_issue": minimal_issue,
    "attachment": attachment,
    "links_and_subtasks": links_and_subtasks,
    "comments": comments,
    "many_comments": many_comments,
    "comment_changes": comment_changes,
}


def seed(client: JiraClient, project: str, cases: List[str], apply: bool) -> None:
    for case in cases:
        label = f"{SEED_LABEL}-{case.replace('_', '-')}"
        existing = search_ids(client, f'labels = "{label}"')
        if existing:
            print(f"{case}: exists ({len(existing)} issues)")
        elif apply:
            print(f"{case}: creating. {CASES[case].__doc__}")
            CASES[case](client, project, case.replace("_", "-"))
        else:
            print(f"{case}: missing. {CASES[case].__doc__}")


def delete(client: JiraClient) -> None:
    for issue_id in search_ids(client, f'labels = "{SEED_LABEL}"'):
        try:
            request(
                client, "DELETE", f"issue/{issue_id}", params={"deleteSubtasks": "true"}
            )
        except HTTPError as http_ex:
            # subtasks are deleted with their parent
            if http_ex.response.status_code != 404:
                raise
    print("deleted all seeded issues")


def anonymize(captured: Any, site_host: str) -> Any:
    """Replaces account ids, names, emails, avatars and the site host in captured responses"""
    aliases: Dict[str, str] = {}

    def _anonymize(value: Any) -> Any:
        if isinstance(value, list):
            return [_anonymize(item) for item in value]
        if not isinstance(value, dict):
            return value
        value = {key: _anonymize(item) for key, item in value.items()}
        if "accountId" in value:
            alias = aliases.setdefault(
                value["accountId"], f"account-{len(aliases) + 1}"
            )
            value["accountId"] = alias
            if "displayName" in value:
                value["displayName"] = f"User {alias}"
            if "emailAddress" in value:
                value["emailAddress"] = f"{alias}@example.com"
            if "avatarUrls" in value:
                value["avatarUrls"] = {
                    size: f"https://avatar.example.com/{alias}/{size}"
                    for size in value["avatarUrls"]
                }
        return value

    text = json.dumps(_anonymize(captured), ensure_ascii=False)
    # account ids are also in urls and user mentions
    for account_id, alias in aliases.items():
        text = text.replace(quote(account_id, safe=""), alias).replace(
            account_id, alias
        )
    return json.loads(text.replace(site_host, "test.atlassian.net"))


def seed_manifest(client: JiraClient) -> List[Dict[str, Any]]:
    """Lists all seeded issues with their key, labels, created and updated"""
    manifest = []
    for page in client.search(
        f'labels = "{SEED_LABEL}"', "key,labels,created,updated", MAX_SEARCH_PAGE_SIZE
    ):
        for issue in page:
            fields = issue.get("fields") or {}
            manifest.append(
                {
                    "id": issue["id"],
                    "key": issue.get("key") or fields.get("key"),
                    "labels": fields.get("labels") or [],
                    "created": fields["created"],
                    "updated": fields["updated"],
                }
            )
    return sorted(manifest, key=lambda issue: int(issue["id"]))


def capture(client: JiraClient) -> None:
    """Captures field metadata, bulk fetched issues with nested values of all known shapes,
    comments and the manifest of seeded issues
    """
    fields = client.fields()
    ids: List[str] = []
    for case in CAPTURED_CASES:
        ids.extend(search_ids(client, f'labels = "{SEED_LABEL}-{case}"'))
    for field in fields:
        custom_type = ((field.get("schema") or {}).get("custom") or "").split(":")[-1]
        if custom_type in CAPTURED_CUSTOM_TYPES:
            clause = f"cf[{field['schema']['customId']}]"
            try:
                ids.extend(search_ids(client, f"{clause} is not EMPTY")[:1])
            except HTTPError:
                print(f"  {field['name']} ({clause}) cannot be searched, skipped")
    field_ids = [
        field["id"] for field in fields if field["id"] not in ("comment", "worklog")
    ]
    issues = request(
        client,
        "POST",
        "issue/bulkfetch",
        json={"issueIdsOrKeys": list(dict.fromkeys(ids)), "fields": field_ids},
    )["issues"]
    for issue in issues:
        issue["fields"] = {k: v for k, v in issue["fields"].items() if v is not None}
    loaded_fields = {field_id for issue in issues for field_id in issue["fields"]}
    commented = request(
        client,
        "POST",
        "issue/bulkfetch",
        json={
            "issueIdsOrKeys": search_ids(
                client, f'labels = "{SEED_LABEL}-{CAPTURED_COMMENTS_CASE}"'
            ),
            "fields": ["comment"],
        },
    )["issues"]
    captured = {
        "fields": [field for field in fields if field["id"] in loaded_fields],
        "issues": issues,
        # comments embedded in issues by issue id
        "comments": {
            issue["id"]: issue["fields"]["comment"]["comments"] for issue in commented
        },
        "seed": seed_manifest(client),
    }
    site_host = urlparse(client.base_url).netloc
    with open(CAPTURED_RESPONSES, "w", encoding="utf-8") as f:
        json.dump(anonymize(captured, site_host), f, indent=1, ensure_ascii=False)
        f.write("\n")
    print(
        f"captured {len(issues)} issues with {len(captured['fields'])} fields and the"
        f" manifest of {len(captured['seed'])} seeded issues"
    )


def main(args: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="create missing cases")
    parser.add_argument(
        "--delete", action="store_true", help="delete all seeded issues"
    )
    parser.add_argument(
        "--capture", action="store_true", help="capture responses for offline tests"
    )
    parser.add_argument(
        "--cases", default=",".join(CASES), help="comma separated cases"
    )
    parser.add_argument(
        "--project", default="TST", help="key of a company-managed project"
    )
    parsed = parser.parse_args(args)
    client = JiraClient(
        dlt.secrets["sources.jira.subdomain"],
        dlt.secrets["sources.jira.email"],
        dlt.secrets["sources.jira.api_token"],
    )
    if parsed.delete:
        delete(client)
    elif parsed.capture:
        capture(client)
    else:
        seed(client, parsed.project, parsed.cases.split(","), parsed.apply)


if __name__ == "__main__":
    main()
