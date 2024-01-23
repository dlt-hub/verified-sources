from dlt.sources.helpers.requests import Response
from api_client import BasePaginator

class NotionPaginator(BasePaginator):
    def __init__(self, cursor_key='next_cursor', records_key='results'):
        super().__init__()
        self.cursor_key = cursor_key
        self.records_key = records_key

    def update_state(self, response: Response):
        self.next_cursor = response.json().get(self.cursor_key)
        self._has_next_page = self.next_cursor is not None

    def prepare_next_request_args(self, url, params, json):
        json = json or {}

        if self.next_cursor:
            json["start_cursor"] = self.next_cursor

        return url, params, json

    def extract_records(self, response: Response):
        return response.json().get(self.records_key, [])