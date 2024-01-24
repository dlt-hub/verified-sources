from dlt.sources.helpers.requests import Response
from .. import settings
from api_client import JSONResponsePaginator


class CursorPaginator(JSONResponsePaginator):
    def get_next_page_url(self, response_json):
        if response_json["meta"]["has_more"]:
            return response_json["links"]["next"]
        else:
            return None

    def update_state(self, response: Response):
        self.next_reference = self.get_next_page_url(response.json())

    def prepare_next_request_args(self, url, params, json):
        params = params or {}
        params["page[size]"] = settings.PAGE_SIZE

        return self.next_reference, params, json


class StreamPaginator(JSONResponsePaginator):
    def get_next_page_url(self, response_json):
        if not response_json["end_of_stream"]:
            return response_json["next_page"]
        else:
            return None

    def update_state(self, response: Response):
        self.next_reference = self.get_next_page_url(response.json())

    def prepare_next_request_args(self, url, params, json):
        params = params or {}
        params["per_page"] = settings.INCREMENTAL_PAGE_SIZE

        return self.next_reference, params, json


class StartTimePaginator(JSONResponsePaginator):
    def get_next_page_url(self, response_json):
        if response_json["count"] > 0:
            return response_json["next_page"]
        else:
            return None

    def update_state(self, response: Response):
        self.next_reference = self.get_next_page_url(response.json())

    def prepare_next_request_args(self, url, params, json):
        params = params or {}
        params["limit"] = settings.INCREMENTAL_PAGE_SIZE

        return self.next_reference, params, json
