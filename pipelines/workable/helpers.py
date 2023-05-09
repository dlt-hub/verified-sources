import requests


def pagination(url: str, endpoint: str, headers: dict, params: dict) -> list:
    url = f"{url}/{endpoint}"

    has_more = True
    while has_more:
        response = requests.get(url, headers=headers, params=params)
        response_json = response.json()
        paging = response_json.get("paging")

        if paging is not None:
            url = paging.get("next")
        else:
            has_more = False

        yield response_json[endpoint]

