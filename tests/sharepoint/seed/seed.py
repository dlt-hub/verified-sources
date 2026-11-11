"""Seeds the Documents library and the `dlt archive` library of the SharePoint test site with the
files of `data.py`.

Run from the repository root. Credentials are resolved from `sources/.dlt` like in tests, and the
app needs `Files.ReadWrite.All`, and `Sites.Manage.All` to create the `dlt archive` library:

    uv run python -m tests.sharepoint.seed.seed

Every run uploads the files again, sets the modification times of the incremental files through
`fileSystemInfo` and deletes files that are no longer in `data.py` from the paging and incremental
folders. The lists are created by hand, see `tests/sharepoint/README.md`.
"""

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime  # noqa: I251
from typing import Dict, Optional, Set, Tuple

# dlt resolves config and secrets from sources/.dlt, like in tests
os.environ.setdefault("DLT_PROJECT_DIR", "sources")

from dlt.common.configuration import resolve_configuration
from dlt.sources.helpers.rest_client import RESTClient

from sources.sharepoint import SharepointCredentials
from sources.sharepoint.graph import site_client

from tests.sharepoint.seed import data
from tests.sharepoint.utils import ensure_folder, item_path, upload

WORKERS = 5


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


if __name__ == "__main__":
    credentials = resolve_configuration(
        SharepointCredentials(), sections=("sources", "sharepoint")
    )
    seed(site_client(**dict(credentials)))
