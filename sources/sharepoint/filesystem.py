"""Read-only fsspec filesystem over one SharePoint document library."""

import unicodedata
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from dlt.common.storages.configuration import MAKE_URI_DISPATCH
from dlt.common.storages.fsspec_filesystem import MTIME_DISPATCH
from dlt.common.time import ensure_datetime_in_tz
from dlt.sources.helpers.requests import HTTPError
from dlt.sources.helpers.rest_client import RESTClient
from fsspec import AbstractFileSystem, register_implementation
from fsspec.spec import AbstractBufferedFile

from .graph import entra_auth, graph_client


PREFIX = "sharepoint://"
ITEM_FIELDS = "id,name,size,file,folder,lastModifiedDateTime"
INVALID_PATH_CHARS = set('"*:<>?\\|')
GLOB_CHARS = set("[]")


class SharepointFileSystem(AbstractFileSystem):
    """Lists and reads the files of a document library through Microsoft Graph.

    Paths are relative to the library root. `library` is the library's display name, None for
    the site's default library. Full urls are sharepoint://<library>/<path>. Pass a Graph
    client, or the Entra app credentials and the site id as fsspec storage options; fsspec
    keeps storage options on the instance, so do not log or pickle it.
    """

    protocol = "sharepoint"

    def __init__(
        self,
        client: Optional[RESTClient] = None,
        library: Optional[str] = None,
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        site_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if client is None:
            if not (tenant_id and client_id and client_secret and site_id):
                raise ValueError(
                    "Pass a Graph client, or tenant_id, client_id, client_secret and site_id"
                )
            client = graph_client(
                site_id, entra_auth(tenant_id, client_id, client_secret)
            )
        self.client = client
        self.library = library
        self._drive: Optional[str] = None

    @property
    def drive(self) -> str:
        """Graph path of the drive, resolved on first use."""
        if self._drive is None:
            self._drive = "drive" if self.library is None else self._find_drive()
        return self._drive

    def _find_drive(self) -> str:
        names = []
        for page in self.client.paginate("drives"):
            for drive in page:
                if drive["name"] == self.library:
                    return f"drives/{drive['id']}"
                names.append(drive["name"])
        raise ValueError(
            f"Document library '{self.library}' not found. Available libraries: {names}"
        )

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        """Returns the path relative to the library root, "" for the root.

        Urls are split by hand: "#" is allowed in SharePoint names, and a url parser would
        read it as a fragment.
        """
        if path.startswith(PREFIX):
            path = path[len(PREFIX) :].partition("/")[2]
        return path.strip("/")

    def _path(self, path: str) -> str:
        """Returns the path relative to the library root, refusing a url of another library."""
        if path.startswith(PREFIX):
            library = path[len(PREFIX) :].partition("/")[0] or None
            if library != self.library:
                raise ValueError(
                    f"'{path}' is not in library '{self.library or 'default'}' of this filesystem"
                )
        return self._strip_protocol(path)

    def walk(
        self,
        path: str,
        maxdepth: Optional[int] = None,
        topdown: bool = True,
        on_error: str = "raise",
        **kwargs: Any,
    ) -> Any:
        """Walks the folder tree, failing on a folder that cannot be listed.

        fsspec drops such folders by default, which would turn a wrong path or a denied
        subfolder into an empty or partial load.
        """
        return super().walk(path, maxdepth, topdown, on_error="raise", **kwargs)

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:  # noqa: A003
        self._path(path)
        return super().open(path, mode, **kwargs)

    def _item_path(self, path: str) -> str:
        if not path:
            return f"{self.drive}/root"
        quoted = "/".join(quote(segment, safe="") for segment in path.split("/"))
        return f"{self.drive}/root:/{quoted}:"

    @staticmethod
    def _entry(parent: str, item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "name": f"{parent}/{item['name']}" if parent else item["name"],
            "size": item.get("size", 0),
            "type": "file" if "file" in item else "directory",
            "modified": ensure_datetime_in_tz(item["lastModifiedDateTime"]),
            "id": item["id"],
        }

    def ls(self, path: str, detail: bool = False, **kwargs: Any) -> List[Any]:
        path = self._path(path)
        try:
            pages = self.client.paginate(
                f"{self._item_path(path)}/children", params={"$select": ITEM_FIELDS}
            )
            entries = [self._entry(path, item) for page in pages for item in page]
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                raise FileNotFoundError(
                    f"Folder '{path}' not found in the library"
                ) from e
            raise
        return entries if detail else [entry["name"] for entry in entries]

    def info(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        path = self._path(path)
        try:
            response = self.client.get(
                self._item_path(path), params={"$select": ITEM_FIELDS}
            )
            response.raise_for_status()
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                raise FileNotFoundError(
                    f"Folder or file '{path}' not found in the library"
                ) from e
            raise
        return self._entry(path.rpartition("/")[0], response.json())

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        **kwargs: Any,
    ) -> "SharepointFile":
        if mode != "rb":
            raise NotImplementedError("SharePoint files are read-only")
        return SharepointFile(self, path, block_size=block_size, **kwargs)


class SharepointFile(AbstractBufferedFile):
    """A file of the library, read in ranges through Graph."""

    def __init__(
        self,
        fs: SharepointFileSystem,
        path: str,
        block_size: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(fs, path, mode="rb", block_size=block_size, **kwargs)
        self.item_id = self.details["id"]

    def _fetch_range(self, start: int, end: int) -> bytes:
        content_path = f"{self.fs.drive}/items/{self.item_id}/content"
        response = self.fs.client.get(
            content_path, headers={"Range": f"bytes={start}-{end - 1}"}
        )
        if not response.ok:
            # the redirected download url carries a short-lived token and must not reach logs
            raise HTTPError(
                f"{response.status_code} error downloading {content_path}",
                response=response,
            )
        return bytes(response.content)


def validate_folder_path(folder_path: str) -> str:
    """Returns the folder path without leading and trailing slashes, "" for the library root.

    Raises:
        ValueError: If the path has an empty segment or a character SharePoint does not allow.
    """
    path = folder_path.strip("/")
    if not path:
        return ""
    for segment in path.split("/"):
        if not segment:
            raise ValueError(
                f"folder_path has an empty segment ('//'): {folder_path!r}"
            )
        invalid = sorted(
            char
            for char in set(segment)
            if char in INVALID_PATH_CHARS or unicodedata.category(char) == "Cc"
        )
        if invalid:
            raise ValueError(
                f"folder_path contains characters SharePoint does not allow {invalid}:"
                f" {folder_path!r}"
            )
        if GLOB_CHARS & set(segment):
            raise ValueError(
                "folder_path cannot contain '[' or ']', which file_glob reads as a pattern:"
                f" {folder_path!r}"
            )
    return path


def make_url(scheme: str, fs_path: str, bucket_url: str) -> str:
    library = bucket_url[len(PREFIX) :].partition("/")[0]
    return f"{scheme}://{library}/{fs_path}"


register_implementation("sharepoint", SharepointFileSystem, clobber=True)
MTIME_DISPATCH["sharepoint"] = lambda entry: entry["modified"]
MAKE_URI_DISPATCH["sharepoint"] = make_url
