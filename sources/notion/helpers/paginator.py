class NotionPaginator:
    def __init__(self, cursor_key='next_cursor', content_key='results'):
        self.cursor_key = cursor_key
        self.content_key = content_key

    def paginate(self, client, url, method, params, json):
        has_more = True
        next_cursor = None

        while has_more:
            json = json or {}
            if next_cursor:
                json["start_cursor"] = next_cursor

            response = client.make_request(path=url, method=method, params=params, json=json)
            response_json = response.json()

            yield response_json.get(self.content_key, [])

            next_cursor = response_json.get(self.cursor_key)
            has_more = next_cursor is not None
