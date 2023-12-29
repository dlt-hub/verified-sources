class Paginator:
    def __init__(self, offset_by_page=False):
        self.offset_by_page = offset_by_page

    def paginate(self, client, url, method, params, json):
        starts_from_zero = False
        while True:
            response = client.make_request(url, method, params, json)

            json_response = response.json()
            yield json_response["data"]

            metadata = json_response.get("metadata")
            if not metadata:
                break

            total_pages = metadata.get("total_pages")
            current_page = metadata.get("current_page")
            if current_page == 0:
                starts_from_zero = True

            if current_page >= (total_pages - int(starts_from_zero)) or not json_response["data"]:
                break

            if self.offset_by_page:
                params["offset"] += 1
                params["page"] += 1
            else:
                params["offset"] += params["limit"]
                params["page"] += 1