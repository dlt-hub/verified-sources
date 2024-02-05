from dlt.sources.helpers.requests import Response
from rest_api import JSONResponsePaginator

class NotionPaginator(JSONResponsePaginator):
    def __init__(self, cursor_key='next_cursor', records_key='results'):
        super().__init__(next_key=cursor_key, records_key=records_key)

    def prepare_next_request_args(self, url, params, json):
        json = json or {}

        if self.next_reference:
            json["start_cursor"] = self.next_reference

        return url, params, json
