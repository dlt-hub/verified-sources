"""Seeds the Documents library of the SharePoint test site with the files of `data.py`.

Run from the repository root. Credentials are resolved from `sources/.dlt` like in tests, and the
app needs `Files.ReadWrite.All`:

    uv run python -m tests.sharepoint.seed.seed

Every run uploads the files again, sets the modification times of the incremental files through
`fileSystemInfo` and deletes files that are no longer in `data.py` from the paging and incremental
folders. The lists are created by hand, see `tests/sharepoint/README.md`.
"""

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime  # noqa: I251
from typing import Dict, Optional, Set, Tuple
from urllib.parse import quote

# dlt resolves config and secrets from sources/.dlt, like in tests
os.environ.setdefault("DLT_PROJECT_DIR", "sources")

from dlt.common.configuration import resolve_configuration
from dlt.sources.helpers.rest_client import RESTClient

from sources.sharepoint import SharepointCredentials
from sources.sharepoint.graph import site_client

from tests.sharepoint.seed import data

WORKERS = 5


def item_path(path: str) -> str:
    quoted = "/".join(quote(segment, safe="") for segment in path.split("/"))
    return f"drive/root:/{quoted}:"


def ensure_folder(client: RESTClient, path: str) -> None:
    if not path or client.get(item_path(path)).ok:
        return
    parent, _, name = path.rpartition("/")
    ensure_folder(client, parent)
    children = f"{item_path(parent)}/children" if parent else "drive/root/children"
    client.post(children, json={"name": name, "folder": {}}).raise_for_status()


def upload(
    client: RESTClient, path: str, content: bytes, modified: Optional[datetime]
) -> None:
    response = client.request(
        f"{item_path(path)}/content",
        method="PUT",
        data=content,
        headers={"Content-Type": "application/octet-stream"},
    )
    response.raise_for_status()
    if modified is not None:
        stamp = modified.isoformat().replace("+00:00", "Z")
        times = {"createdDateTime": stamp, "lastModifiedDateTime": stamp}
        client.request(
            f"drive/items/{response.json()['id']}",
            method="PATCH",
            json={"fileSystemInfo": times},
        ).raise_for_status()


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


if __name__ == "__main__":
    credentials = resolve_configuration(
        SharepointCredentials(), sections=("sources", "sharepoint")
    )
    seed(site_client(**dict(credentials)))
