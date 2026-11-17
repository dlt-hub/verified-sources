"""Seeds the Documents library and the `dlt archive` library of the SharePoint test site with the
files of `data.py`, or the performance list with `--performance`.

Run from the repository root. Credentials are resolved from `sources/.dlt` like in tests, and the
app needs `Files.ReadWrite.All`, and `Sites.Manage.All` to create the `dlt archive` library and the
performance list:

    uv run python -m tests.sharepoint.seed.seed
    uv run python -m tests.sharepoint.seed.seed --performance

Every run uploads the files again, sets the modification times of the incremental files through
`fileSystemInfo` and deletes files that are no longer in `data.py` from the paging and incremental
folders. The lists of the read tests are created by hand, see `tests/sharepoint/README.md`.

`--performance` creates the `dlt perf` list with its columns when missing, adds the items it does not
have yet and deletes duplicates, so it resumes after an interruption.
"""

import argparse
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime  # noqa: I251
from functools import partial
from time import sleep
from typing import Any, Dict, List, Optional, Set, Tuple

# dlt resolves config and secrets from sources/.dlt, like in tests
os.environ.setdefault("DLT_PROJECT_DIR", "sources")

from dlt.common.configuration import resolve_configuration
from dlt.sources.helpers.rest_client import RESTClient

from sources.sharepoint import SharepointCredentials
from sources.sharepoint.graph import GRAPH_URL, USER_AGENT, entra_auth, site_client

from tests.seed_utils import account_lock, map_with_progress
from tests.sharepoint.seed import data
from tests.sharepoint.utils import ensure_folder, item_path, upload

WORKERS = 5
# requests in one Graph `$batch`, the most it accepts
BATCH_SIZE = 20
RETRY_STATUSES = (429, 503, 504)


def ensure_library(client: RESTClient, name: str) -> str:
    """Returns the Graph path of the drive of document library `name`, created when missing"""
    for page in client.paginate("drives", params={"$select": "id,name"}):
        for drive in page:
            if drive["name"] == name:
                return f"drives/{drive['id']}"
    created = client.post(
        "lists", json={"displayName": name, "list": {"template": "documentLibrary"}}
    )
    created.raise_for_status()
    drive = client.get(f"lists/{created.json()['id']}/drive")
    drive.raise_for_status()
    return f"drives/{drive.json()['id']}"


def seed_archive(client: RESTClient) -> None:
    drive = ensure_library(client, data.ARCHIVE_LIBRARY)
    ensure_folder(client, data.ARCHIVE_FILE.rpartition("/")[0], drive)
    upload(client, data.ARCHIVE_FILE, data.ARCHIVE_CONTENT, None, drive)
    print(f"seeded {data.ARCHIVE_FILE} in {data.ARCHIVE_LIBRARY}")


def prune(client: RESTClient, folder: str, keep: Set[str]) -> None:
    pages = client.paginate(
        f"{item_path(folder)}/children", params={"$select": "id,name"}
    )
    for page in pages:
        for item in page:
            if item["name"] not in keep:
                client.request(
                    f"drive/items/{item['id']}", method="DELETE"
                ).raise_for_status()


def seed(client: RESTClient) -> None:
    files: Dict[str, Tuple[bytes, Optional[datetime]]] = {
        path: (content, None)
        for path, content in {**data.DOCUMENTS, **data.paging_files()}.items()
    }
    files.update(
        {
            f"{data.INCREMENTAL_FOLDER}/{name}": (content, modified)
            for name, content, modified in data.incremental_files()
        }
    )
    for folder in sorted({path.rpartition("/")[0] for path in files}):
        ensure_folder(client, folder)
    with ThreadPoolExecutor(WORKERS) as pool:
        list(pool.map(lambda item: upload(client, item[0], *item[1]), files.items()))
    for folder in (data.PAGING_FOLDER, data.INCREMENTAL_FOLDER):
        names = {
            path.rpartition("/")[2] for path in files if path.startswith(f"{folder}/")
        }
        prune(client, folder, names)
    print(f"seeded {len(files)} files")
    seed_archive(client)


def ensure_perf_list(client: RESTClient) -> str:
    """Returns the id of the performance list, created with its columns when missing"""
    for page in client.paginate("lists", params={"$select": "id,displayName"}):
        for site_list in page:
            if site_list["displayName"] == data.PERF_LIST:
                return str(site_list["id"])
    created = client.post(
        "lists",
        json={
            "displayName": data.PERF_LIST,
            "columns": data.PERF_COLUMNS,
            "list": {"template": "genericList"},
        },
    )
    created.raise_for_status()
    return str(created.json()["id"])


def perf_item_ids(client: RESTClient, list_id: str) -> Dict[int, List[str]]:
    """Returns the ids of the items of the performance list by their `Number`"""
    ids: Dict[int, List[str]] = defaultdict(list)
    params = {"$expand": "fields($select=Number)", "$top": 1000}
    for page in client.paginate(f"lists/{list_id}/items", params=params):
        for item in page:
            number = item["fields"].get("Number")
            if number is not None:
                ids[int(number)].append(item["id"])
    return ids


def send_batch(batch_client: RESTClient, requests: List[Dict[str, Any]]) -> None:
    """Sends `requests` in one Graph `$batch` and repeats the throttled ones"""
    while requests:
        response = batch_client.post("$batch", json={"requests": requests})
        response.raise_for_status()
        by_id = {request["id"]: request for request in requests}
        requests, wait = [], 0
        for answer in response.json()["responses"]:
            if answer["status"] in RETRY_STATUSES:
                requests.append(by_id[answer["id"]])
                wait = max(wait, int(answer.get("headers", {}).get("Retry-After", 5)))
            elif answer["status"] >= 400:
                raise ValueError(
                    f"Request {answer['id']} of a batch failed with {answer['status']}:"
                    f" {answer.get('body')}"
                )
        sleep(wait)


def seed_performance(
    client: RESTClient, batch_client: RESTClient, site_id: str
) -> None:
    list_id = ensure_perf_list(client)
    items_url = f"/sites/{site_id}/lists/{list_id}/items"
    ids = perf_item_ids(client, list_id)
    requests: List[Dict[str, Any]] = [
        {
            "id": str(n),
            "method": "POST",
            "url": items_url,
            "headers": {"Content-Type": "application/json"},
            "body": {"fields": data.perf_item(n)},
        }
        for n in range(data.PERF_ITEMS)
        if n not in ids
    ]
    # an item created by a request that also failed on the client is there twice
    requests += [
        {"id": item_id, "method": "DELETE", "url": f"{items_url}/{item_id}"}
        for item_ids in ids.values()
        for item_id in item_ids[1:]
    ]
    batches = [
        requests[start : start + BATCH_SIZE]
        for start in range(0, len(requests), BATCH_SIZE)
    ]
    print(f"{len(ids)} items in {data.PERF_LIST}, sending {len(requests)} requests")
    map_with_progress(
        partial(send_batch, batch_client),
        batches,
        data.PERF_LIST,
        workers=WORKERS,
        weight=len,
    )
    seeded = perf_item_ids(client, list_id)
    duplicates = sum(len(item_ids) - 1 for item_ids in seeded.values())
    print(f"{len(seeded)} of {data.PERF_ITEMS} items, {duplicates} duplicates")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--performance", action="store_true")
    args = parser.parse_args()
    credentials = resolve_configuration(
        SharepointCredentials(), sections=("sources", "sharepoint")
    )
    site = site_client(**dict(credentials))
    if args.performance:
        batch_client = RESTClient(
            base_url=GRAPH_URL,
            headers={"User-Agent": USER_AGENT},
            auth=entra_auth(
                credentials.tenant_id, credentials.client_id, credentials.client_secret
            ),
        )
        with account_lock("sharepoint"):
            seed_performance(site, batch_client, credentials.site_id)
    else:
        seed(site)
