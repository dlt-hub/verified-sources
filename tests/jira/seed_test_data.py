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
    uv run python ../tests/jira/seed_test_data.py --capture  # captures responses for offline tests
"""

import argparse
import json
import os
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote

import dlt
from dlt.sources.helpers.requests import Client, HTTPError, Response

SEED_LABEL = "dlt-seed"
BULK_ISSUES = 250
CHANGELOG_EDITS = 120
WORKLOGS = 25
COMMENTS = 120
CAPTURE_FILE = os.path.join(os.path.dirname(__file__), "captured_responses.json")
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


class Jira:
    """Calls Jira REST API v2 and raises errors with the messages returned by Jira"""

    def __init__(
        self, subdomain: str, email: str, api_token: str, project: str
    ) -> None:
        self.site_host = f"{subdomain}.atlassian.net"
        self.base_url = f"https://{self.site_host}/rest/api/2/"
        self.session = Client(
            raise_for_status=False, session_attrs={"auth": (email, api_token)}
        ).session
        self.project = project
        self.account_id: str = self.request("GET", "myself")["accountId"]

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = {"Accept": "application/json", **kwargs.pop("headers", {})}
        response: Response = self.session.request(
            method, self.base_url + path, headers=headers, **kwargs
        )
        if response.status_code >= 400:
            raise HTTPError(
                f"{method} {path} failed with {response.status_code}: {response.text}",
                response=response,
            )
        return response.json() if response.content else None

    def fields(self, case: str, summary: str, **fields: Any) -> Dict[str, Any]:
        return {
            "project": {"key": self.project},
            "issuetype": {"name": "Task"},
            "summary": f"[{SEED_LABEL}] {summary}",
            "labels": [SEED_LABEL, f"{SEED_LABEL}-{case}"],
            **fields,
        }

    def create_issue(self, case: str, summary: str, **fields: Any) -> str:
        key: str = self.request(
            "POST", "issue", json={"fields": self.fields(case, summary, **fields)}
        )["key"]
        print(f"  created {key}: {summary}")
        return key

    def search_ids(self, jql: str) -> List[str]:
        ids: List[str] = []
        params: Dict[str, Any] = {"jql": jql, "fields": "id", "maxResults": 5000}
        while True:
            page = self.request("GET", "search/jql", params=params)
            ids.extend(issue["id"] for issue in page["issues"])
            if page.get("isLast") or not page.get("nextPageToken"):
                return ids
            params["nextPageToken"] = page["nextPageToken"]

    def subtask_type(self) -> str:
        issue_types = self.request("GET", f"project/{self.project}")["issueTypes"]
        name: str = next(t["name"] for t in issue_types if t.get("subtask"))
        return name


def bulk_issues(jira: Jira, case: str) -> None:
    """More issues than a bulk fetch batch of 100, created within the same minutes"""
    for start in range(0, BULK_ISSUES, 50):
        updates = [
            {"fields": jira.fields(case, f"bulk issue {n + 1}")}
            for n in range(start, min(start + 50, BULK_ISSUES))
        ]
        result = jira.request("POST", "issue/bulk", json={"issueUpdates": updates})
        print(
            f"  created {len(result['issues'])} issues, errors: {result.get('errors')}"
        )


def long_changelog(jira: Jira, case: str) -> None:
    """More changes than the changelog embedded in an issue and than a changelog page"""
    key = jira.create_issue(case, "long changelog")
    for n in range(CHANGELOG_EDITS):
        summary = f"[{SEED_LABEL}] long changelog, edit {n + 1}"
        jira.request("PUT", f"issue/{key}", json={"fields": {"summary": summary}})
    print(f"  edited {key} {CHANGELOG_EDITS} times")


def status_transitions(jira: Jira, case: str) -> None:
    """Status changes in the changelog, walks the available transitions"""
    key = jira.create_issue(case, "status transitions")
    for n in range(6):
        transitions = jira.request("GET", f"issue/{key}/transitions")["transitions"]
        # rotate through transitions, some of them may require a screen and fail
        for transition in transitions[n % len(transitions) :] + transitions:
            try:
                jira.request(
                    "POST",
                    f"issue/{key}/transitions",
                    json={"transition": {"id": transition["id"]}},
                )
                print(f"  {key} -> {transition['to']['name']}")
                break
            except HTTPError:
                continue


def many_worklogs(jira: Jira, case: str) -> None:
    """More worklogs than the 20 embedded in an issue, started in different time zones"""
    key = jira.create_issue(case, "many worklogs")
    offsets = ["+0000", "+0530", "-0800", "+0100"]
    for n in range(WORKLOGS):
        worklog = {
            "started": f"2026-01-{n + 1:02d}T09:00:00.000{offsets[n % len(offsets)]}",
            "timeSpentSeconds": 900 * (n + 1),
            "comment": f"worklog {n + 1}: ünïcode ✓",
        }
        jira.request("POST", f"issue/{key}/worklog", json=worklog)
    print(f"  logged work {WORKLOGS} times on {key}")


def deleted_worklog(jira: Jira, case: str) -> None:
    """A deleted worklog, listed by the deleted worklogs feed"""
    key = jira.create_issue(case, "deleted worklog")
    started = "2026-01-01T09:00:00.000+0000"
    for comment in ("kept", "deleted"):
        worklog = jira.request(
            "POST",
            f"issue/{key}/worklog",
            json={"started": started, "timeSpentSeconds": 600, "comment": comment},
        )
    jira.request("DELETE", f"issue/{key}/worklog/{worklog['id']}")
    print(f"  deleted worklog {worklog['id']} of {key}")


def comments(jira: Jira, case: str) -> None:
    """Comments with wiki markup and unicode, an edited, a restricted and a deleted comment"""
    key = jira.create_issue(case, "comments")
    path = f"issue/{key}/comment"
    body = "h2. Seeded\n*bold* comment with emoji 🚀, ünïcode and a\nnew line"
    jira.request("POST", path, json={"body": body})
    edited = jira.request("POST", path, json={"body": "before edit"})
    jira.request("PUT", f"{path}/{edited['id']}", json={"body": "after edit"})
    # requires the user to have the role, group visibility may be disabled on the site
    visibility = {"type": "role", "value": "Administrators"}
    try:
        jira.request(
            "POST", path, json={"body": "restricted", "visibility": visibility}
        )
    except HTTPError as http_ex:
        print(f"  restricted comment skipped: {http_ex}")
    deleted = jira.request("POST", path, json={"body": "deleted"})
    jira.request("DELETE", f"{path}/{deleted['id']}")
    print(f"  commented {key}")


def many_comments(jira: Jira, case: str) -> None:
    """More comments than a page of comments embedded in an issue"""
    key = jira.create_issue(case, "many comments")
    for n in range(COMMENTS):
        jira.request("POST", f"issue/{key}/comment", json={"body": f"comment {n + 1}"})
    print(f"  commented {key} {COMMENTS} times")


def comment_changes(jira: Jira, case: str) -> None:
    """An issue on which online tests add, edit and delete their comments"""
    jira.create_issue(case, "comment changes")


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


def all_field_types(jira: Jira, case: str) -> None:
    """Sets every field that can be set on create: custom field types and system fields"""
    issue_types = jira.request("GET", f"issue/createmeta/{jira.project}/issuetypes")
    task_type = next(t for t in issue_types["issueTypes"] if t["name"] == "Task")
    meta = jira.request(
        "GET", f"issue/createmeta/{jira.project}/issuetypes/{task_type['id']}"
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
        value = field_value(field, jira.account_id)
        if field["fieldId"] not in skip and value is not None:
            fields[field["fieldId"]] = value
    try:
        jira.create_issue(case, "all field types", **fields)
    except HTTPError as http_ex:
        # drop fields rejected by Jira and try again
        errors = http_ex.response.json().get("errors") or {}
        print(f"  dropping rejected fields: {errors}")
        jira.create_issue(
            case,
            "all field types",
            **{k: v for k, v in fields.items() if k not in errors},
        )


def minimal_issue(jira: Jira, case: str) -> None:
    """An issue with required fields only, all other fields are empty"""
    jira.create_issue(case, "minimal issue")


def attachment(jira: Jira, case: str) -> None:
    """An issue with an attachment"""
    key = jira.create_issue(case, "attachment")
    jira.request(
        "POST",
        f"issue/{key}/attachments",
        headers={"X-Atlassian-Token": "no-check"},
        files={"file": ("dlt-seed.csv", "id,name\n1,ünïcode\n".encode(), "text/csv")},
    )
    print(f"  attached a file to {key}")


def links_and_subtasks(jira: Jira, case: str) -> None:
    """Subtasks of a parent and issue links in both directions"""
    parent = jira.create_issue(case, "links parent")
    subtask_type = jira.subtask_type()
    for n in (1, 2):
        jira.create_issue(
            case,
            f"subtask {n}",
            issuetype={"name": subtask_type},
            parent={"key": parent},
        )
    other = jira.create_issue(case, "links other")
    for link_type in ("Blocks", "Relates"):
        link = {
            "type": {"name": link_type},
            "inwardIssue": {"key": parent},
            "outwardIssue": {"key": other},
        }
        jira.request("POST", "issueLink", json=link)
    print(f"  linked {parent} and {other}")


CASES: Dict[str, Callable[[Jira, str], None]] = {
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


def seed(jira: Jira, cases: List[str], apply: bool) -> None:
    for case in cases:
        label = f"{SEED_LABEL}-{case.replace('_', '-')}"
        existing = jira.search_ids(f'labels = "{label}"')
        if existing:
            print(f"{case}: exists ({len(existing)} issues)")
        elif apply:
            print(f"{case}: creating. {CASES[case].__doc__}")
            CASES[case](jira, case.replace("_", "-"))
        else:
            print(f"{case}: missing. {CASES[case].__doc__}")


def delete(jira: Jira) -> None:
    for issue_id in jira.search_ids(f'labels = "{SEED_LABEL}"'):
        try:
            jira.request(
                "DELETE", f"issue/{issue_id}", params={"deleteSubtasks": "true"}
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


def capture(jira: Jira) -> None:
    """Captures field metadata, bulk fetched issues with nested values of all known shapes and
    comments
    """
    fields = jira.request("GET", "field")
    ids: List[str] = []
    for case in CAPTURED_CASES:
        ids.extend(jira.search_ids(f'labels = "{SEED_LABEL}-{case}"'))
    for field in fields:
        custom_type = ((field.get("schema") or {}).get("custom") or "").split(":")[-1]
        if custom_type in CAPTURED_CUSTOM_TYPES:
            clause = f"cf[{field['schema']['customId']}]"
            try:
                ids.extend(jira.search_ids(f"{clause} is not EMPTY")[:1])
            except HTTPError:
                print(f"  {field['name']} ({clause}) cannot be searched, skipped")
    field_ids = [
        field["id"] for field in fields if field["id"] not in ("comment", "worklog")
    ]
    issues = jira.request(
        "POST",
        "issue/bulkfetch",
        json={"issueIdsOrKeys": list(dict.fromkeys(ids)), "fields": field_ids},
    )["issues"]
    for issue in issues:
        issue["fields"] = {k: v for k, v in issue["fields"].items() if v is not None}
    loaded_fields = {field_id for issue in issues for field_id in issue["fields"]}
    commented = jira.request(
        "POST",
        "issue/bulkfetch",
        json={
            "issueIdsOrKeys": jira.search_ids(
                f'labels = "{SEED_LABEL}-{CAPTURED_COMMENTS_CASE}"'
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
    }
    with open(CAPTURE_FILE, "w", encoding="utf-8") as f:
        json.dump(anonymize(captured, jira.site_host), f, indent=1, ensure_ascii=False)
        f.write("\n")
    print(f"captured {len(issues)} issues with {len(captured['fields'])} fields")


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
    jira = Jira(
        dlt.secrets["sources.jira.subdomain"],
        dlt.secrets["sources.jira.email"],
        dlt.secrets["sources.jira.api_token"],
        parsed.project,
    )
    if parsed.delete:
        delete(jira)
    elif parsed.capture:
        capture(jira)
    else:
        seed(jira, parsed.cases.split(","), parsed.apply)


if __name__ == "__main__":
    main()
