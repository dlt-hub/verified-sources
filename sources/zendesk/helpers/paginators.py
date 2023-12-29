from .. import settings
from api_client import BasePaginator

class BaseZendeskPaginator(BasePaginator):
    def __init__(self, content_key="results"):
        self.content_key = content_key

    def paginate(self, client, url, method, params, json):
        raise NotImplementedError

    def make_paginated_request(self, client, url, method, params, json):
        while url:
            response = client.make_request(url, method, params, json)
            response_json = response.json()

            yield response_json[self.content_key]
            url = self.get_next_page_url(response_json)

    def get_next_page_url(self, response_json):
        raise NotImplementedError


class CursorPaginator(BaseZendeskPaginator):
    def get_next_page_url(self, response_json):
        if response_json["meta"]["has_more"]:
            return response_json["links"]["next"]
        else:
            return None

    def paginate(self, client, url, method, params, json):
        params = params or {}
        params["page[size]"] = settings.PAGE_SIZE

        return self.make_paginated_request(client, url, method, params, json)


class StreamPaginator(BaseZendeskPaginator):
    def get_next_page_url(self, response_json):
        if not response_json["end_of_stream"]:
            return response_json["next_page"]
        else:
            return None

    def paginate(self, client, url, method, params, json):
        params = params or {}
        params["per_page"] = settings.INCREMENTAL_PAGE_SIZE

        return self.make_paginated_request(client, url, method, params, json)


class StartTimePaginator(BaseZendeskPaginator):
    def get_next_page_url(self, response_json):
        if response_json["count"] > 0:
            return response_json["next_page"]
        else:
            return None

    def paginate(self, client, url, method, params, json):
        params = params or {}
        params["limit"] = settings.INCREMENTAL_PAGE_SIZE

        return self.make_paginated_request(client, url, method, params, json)