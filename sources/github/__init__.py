"""Source that load github issues, pull requests and reactions for a specific repository via customizable graphql query. Loads events incrementally."""
import urllib.parse
from typing import Any, Iterator, List, Sequence, Tuple

import dlt
from dlt.common.typing import StrAny, DictStrAny, TDataItems
from dlt.common.utils import chunks
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests

from .queries import ISSUES_QUERY, RATE_LIMIT, COMMENT_REACTIONS_QUERY


@dlt.source
def github_reactions(
    owner: str,
    name: str,
    access_token: str = dlt.secrets.value,
    items_per_page: int = 100,
    max_items: int = None,
    max_item_age_seconds: float = None,
) -> Sequence[DltResource]:
    """Get reactions associated with issues, pull requests and comments in the repo `name` with owner `owner`

    This source uses graphql to retrieve all issues (`issues` resource) and pull requests (`pull requests` resource) with the associated reactions (up to 100),
    comments (up to 100) and reactions to comments (also up to 100). Internally graphql is used to retrieve data. It is cost optimized and you are able to retrieve the
    data for fairly large repos quickly and cheaply.
    You can and should change the queries in `queries.py` to include for example additional fields or connections. The source can be hacked to add more resources for other
    repository nodes easily.

    Args:
        owner (str): The repository owner
        name (str): The repository name
        access_token (str): The classic access token. Will be injected from secrets if not provided.
        items_per_page (int, optional): How many issues/pull requests to get in single page. Defaults to 100.
        max_items (int, optional): How many issues/pull requests to get in total. None means All.
        max_item_age_seconds (float, optional): Do not get items older than this. Defaults to None. NOT IMPLEMENTED

    Returns:
        Sequence[DltResource]: Two DltResources: `issues` with issues and `pull_requests` with pull requests
    """
    return (
        dlt.resource(
            _get_reactions_data(
                "issues",
                owner,
                name,
                access_token,
                items_per_page,
                max_items,
                max_item_age_seconds,
            ),
            name="issues",
            write_disposition="replace",
        ),
        dlt.resource(
            _get_reactions_data(
                "pullRequests",
                owner,
                name,
                access_token,
                items_per_page,
                max_items,
                max_item_age_seconds,
            ),
            name="pull_requests",
            write_disposition="replace",
        ),
    )


@dlt.source(max_table_nesting=2)
def github_repo_events(owner: str, name: str, access_token: str = None) -> DltResource:
    """Gets events for repository `name` with owner `owner` incrementally.

    This source contains a single resource `repo_events` that gets given repository's events and dispatches them to separate tables with names based on event type.
    The data is loaded incrementally. Subsequent runs will get only new events and append them to tables.
    Please note that Github allows only for 300 events to be retrieved for public repositories. You should get the events frequently for the active repos.

    Args:
        owner (str): The repository owner
        name (str): The repository name
        access_token (str): The classic or fine-grained access token. If not provided, calls are made anonymously

    Returns:
        DltSource: source with the `repo_events` resource

    """

    # use naming function in table name to generate separate tables for each event
    @dlt.resource(primary_key="id", table_name=lambda i: i["type"])  # type: ignore
    def repo_events(
        last_created_at: dlt.sources.incremental[str] = dlt.sources.incremental(
            "created_at", initial_value="1970-01-01T00:00:00Z", last_value_func=max
        )
    ) -> Iterator[TDataItems]:
        repos_path = "/repos/%s/%s/events" % (
            urllib.parse.quote(owner),
            urllib.parse.quote(name),
        )

        for page in _get_rest_pages(access_token, repos_path + "?per_page=100"):
            yield page

            # stop requesting pages if the last element was already older than initial value
            # note: incremental will skip those items anyway, we just do not want to use the api limits
            if last_created_at.start_out_of_range:
                print(
                    f"Overlap with previous run created at {last_created_at.initial_value}"
                )
                break

    return repo_events


def _get_reactions_data(
    node_type: str,
    owner: str,
    name: str,
    access_token: str,
    items_per_page: int,
    max_items: int,
    max_item_age_seconds: float = None,
) -> Iterator[Iterator[StrAny]]:
    variables = {
        "owner": owner,
        "name": name,
        "issues_per_page": items_per_page,
        "first_reactions": 100,
        "first_comments": 100,
        "node_type": node_type,
    }
    for page_items in _get_graphql_pages(
        access_token, ISSUES_QUERY % node_type, variables, node_type, max_items
    ):
        # use reactionGroups to query for reactions to comments that have any reactions. reduces cost by 10-50x
        reacted_comment_ids = {}
        for item in page_items:
            for comment in item["comments"]["nodes"]:
                if any(group["createdAt"] for group in comment["reactionGroups"]):
                    # print(f"for comment {comment['id']}: has reaction")
                    reacted_comment_ids[comment["id"]] = comment
                # if "reactionGroups" in comment:
                comment.pop("reactionGroups", None)

        # get comment reactions by querying comment nodes separately
        comment_reactions = _get_comment_reaction(
            list(reacted_comment_ids.keys()), access_token
        )
        # attach the reaction nodes where they should be
        for comment in comment_reactions.values():
            comment_id = comment["id"]
            reacted_comment_ids[comment_id]["reactions"] = comment["reactions"]
        yield map(_extract_nested_nodes, page_items)


def _extract_top_connection(data: StrAny, node_type: str) -> StrAny:
    assert (
        isinstance(data, dict) and len(data) == 1
    ), f"The data with list of {node_type} must be a dictionary and contain only one element"
    data = next(iter(data.values()))
    return data[node_type]  # type: ignore


def _extract_nested_nodes(item: DictStrAny) -> DictStrAny:
    """Recursively moves `nodes` and `totalCount` to reduce nesting"""

    item["reactions_totalCount"] = item["reactions"].get("totalCount", 0)
    item["reactions"] = item["reactions"]["nodes"]
    comments = item["comments"]
    item["comments_totalCount"] = item["comments"].get("totalCount", 0)
    for comment in comments["nodes"]:
        if "reactions" in comment:
            comment["reactions_totalCount"] = comment["reactions"].get("totalCount", 0)
            comment["reactions"] = comment["reactions"]["nodes"]
    item["comments"] = comments["nodes"]
    return item


def _get_auth_header(access_token: str) -> StrAny:
    if access_token:
        return {"Authorization": f"Bearer {access_token}"}
    else:
        # REST API works without access token (with high rate limits)
        return {}


def _run_graphql_query(
    access_token: str, query: str, variables: DictStrAny
) -> Tuple[StrAny, StrAny]:
    def _request() -> requests.Response:
        r = requests.post(
            "https://api.github.com/graphql",
            json={"query": query, "variables": variables},
            headers=_get_auth_header(access_token),
        )
        return r

    data = _request().json()
    if "errors" in data:
        raise ValueError(data)
    data = data["data"]
    # pop rate limits
    rate_limit = data.pop("rateLimit", {"cost": 0, "remaining": 0})
    return data, rate_limit


def _get_graphql_pages(
    access_token: str, query: str, variables: DictStrAny, node_type: str, max_items: int
) -> Iterator[List[DictStrAny]]:
    items_count = 0
    while True:
        data, rate_limit = _run_graphql_query(access_token, query, variables)
        data_items = _extract_top_connection(data, node_type)["nodes"]
        items_count += len(data_items)
        print(
            f'Got {len(data_items)}/{items_count} {node_type}s, query cost {rate_limit["cost"]}, remaining credits: {rate_limit["remaining"]}'
        )
        if data_items:
            yield data_items
        else:
            return
        # print(data["repository"][node_type]["pageInfo"]["endCursor"])
        variables["page_after"] = _extract_top_connection(data, node_type)["pageInfo"][
            "endCursor"
        ]
        if max_items and items_count >= max_items:
            print(f"Max items limit reached: {items_count} >= {max_items}")
            return


def _get_comment_reaction(comment_ids: List[str], access_token: str) -> StrAny:
    """Builds a query from a list of comment nodes and returns associated reactions"""
    idx = 0
    data: DictStrAny = {}
    for page_chunk in chunks(comment_ids, 50):
        subs = []
        for comment_id in page_chunk:
            subs.append(COMMENT_REACTIONS_QUERY % (idx, comment_id))
            idx += 1
        subs.append(RATE_LIMIT)
        query = "{" + ",\n".join(subs) + "}"
        # print(query)
        page, rate_limit = _run_graphql_query(access_token, query, {})
        print(
            f'Got {len(page)} comments, query cost {rate_limit["cost"]}, remaining credits: {rate_limit["remaining"]}'
        )
        data.update(page)
    return data


def _get_rest_pages(access_token: str, query: str) -> Iterator[List[StrAny]]:
    def _request(url: str) -> requests.Response:
        r = requests.get(url, headers=_get_auth_header(access_token))
        print(f"got page {url}, requests left: " + r.headers["x-ratelimit-remaining"])
        return r

    url = "https://api.github.com" + query
    while True:
        r: requests.Response = _request(url)
        page_items = r.json()
        if len(page_items) == 0:
            break
        yield page_items
        if "next" not in r.links:
            break
        url = r.links["next"]["url"]
