from typing import Dict, List
from io import BytesIO
import re

from msal import ConfidentialClientApplication
from dlt.common import logger
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator


class SharepointClient:
    # * playground:  https://developer.microsoft.com/en-us/graph/graph-explorer
    # * If the result contains more results, Microsoft Graph returns an @odata.nextLink property

    def __init__(
        self,
        client_id: str,
        tenant_id: str,
        site_id: str,
        client_secret: str,
        sub_site_id: str = "",
    ) -> None:
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.client_secret = client_secret
        self.sub_site_id = sub_site_id
        self.site_id = site_id
        if not all([self.client_id, self.tenant_id, self.client_secret, self.site_id]):
            raise ValueError(
                "client_id, tenant_id, client_secret and site_id are required for connect to"
                " SharePoint"
            )
        self.graph_api_url = "https://graph.microsoft.com/v1.0/sites"
        self.graph_site_url = f"{self.graph_api_url}/{self.site_id}"
        if self.sub_site_id:
            self.graph_site_url += f"/sites/{self.sub_site_id}"

    def connect(self) -> None:
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        scope = ["https://graph.microsoft.com/.default"]

        app = ConfidentialClientApplication(
            self.client_id,
            authority=authority,
            client_credential=self.client_secret,
        )

        # Get the access token
        token_response = app.acquire_token_for_client(scopes=scope)
        access_token = token_response.get("access_token", None)

        if access_token:
            self.client = RESTClient(
                base_url=self.graph_site_url,
                auth=BearerTokenAuth(access_token),
                paginator=JSONLinkPaginator(next_url_path="@odata.nextLink"),
            )
            logger.info(f"Connected to SharePoint site id: {self.site_id} successfully")
        else:
            raise ConnectionError("Connection failed : ", token_response)

    @property
    def sub_sites(self) -> List:
        url = f"{self.graph_site_url}/sites"
        response = self.client.get(url)
        site_info = response.json()
        if "value" in site_info:
            return site_info["value"]
        else:
            logger.warning(f"No subsite found in {url}")

    @property
    def site_info(self) -> Dict:
        url = f"{self.graph_site_url}"
        response = self.client.get(url)
        site_info = response.json()
        if not "error" in site_info:
            return site_info
        else:
            logger.warning(f"No site_info found in {url}")

    def get_all_lists_in_site(self) -> List[Dict]:
        url = f"{self.graph_site_url}/lists"
        res = self.client.get(url)
        res.raise_for_status()
        lists_info = res.json()
        if "value" in lists_info:
            all_items = lists_info["value"]
            filtered_lists = [
                item for item in all_items
                if item.get("list", {}).get("template") == "genericList"
                and "Lists" in item.get("webUrl", "")
            ]
            return filtered_lists
        else:
            filtered_lists = []
        if not filtered_lists:
            logger.warning(f"No lists found in {url}")
        return filtered_lists

    def get_items_from_list(self, list_title: str, select:str = None) -> List[Dict]:
        #TODO, pagination not yet implemented
        logger.warning(
            "Pagination is not implemented for get_items_from_list, "
            "it will return only first page of items."
        )
        all_lists = self.get_all_lists_in_site()
        filtered_lists = [
            x for x in all_lists
            if x.get("list", {}).get("template") == "genericList"
            and "Lists" in x.get("webUrl", "")
        ]

        possible_list_titles = [x["displayName"] for x in filtered_lists]
        if list_title not in possible_list_titles:
            raise ValueError(
                f"List with title '{list_title}' not found in site {self.site_id}. "
                f"Available lists: {possible_list_titles}"
            )

        # Get the list ID
        list_id = next(
            x["id"] for x in filtered_lists if x["displayName"] == list_title
        )

        url = f"{self.graph_site_url}/lists/{list_id}/items?expand=fields"
        if select:
            url += f"(select={select})"
        res = self.client.get(url)
        res.raise_for_status()
        items_info = res.json()

        if "value" in items_info:
            output = [x.get("fields", {}) for x in items_info["value"]]
        else:
            output = []
        if output:
            logger.info(f"Got {len(output)} items from list: {list_title}")
            return output
        else:
            logger.warning(f"No items found in list: {list_title}, with select: {select}")

    def get_files_from_path(
        self, folder_path: str, file_name_startswith: str, pattern: str = None
    ) -> Dict:
        folder_url = (
            f"{self.graph_site_url}/drive/root:/{folder_path}:/children?$filter=startswith(name,"
            f" '{file_name_startswith}')"
        )
        logger.debug(f"Getting files from folder with endpoint: {folder_url}")
        res = self.client.get(folder_url)
        file_and_folder_items = res.json().get("value", [])
        file_items = [x for x in file_and_folder_items if "file" in x.keys()]
        if pattern:
            logger.debug(f"Filtering files with pattern: {pattern}")
            file_items = [x for x in file_items if re.search(pattern, x["name"])]

        logger.debug(f"Got number files from ms graph api: {len(file_items)}")
        return file_items

    def get_file_bytes_io(self, file_item: Dict):
        file_url = file_item["@microsoft.graph.downloadUrl"]
        response = self.client.get(file_url)
        if response.status_code == 200:
            bytes_io = BytesIO(response.content)
            logger.info(
                f"File {file_item['name']} downloaded to BytesIO, size: {len(bytes_io.getvalue())}"
            )
            return bytes_io
        else:
            raise FileNotFoundError(f"File not found: {file_item['name']} or can't be downloaded")
