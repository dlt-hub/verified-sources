import pytest
import requests_mock
import re
import json
from urllib.parse import urlsplit, urlunsplit

MOCK_BASE_URL = "https://api.example.com"


# TODO: Accept page_size
def generate_paginated_response(data, page, total_pages, base_url):
    response = {
        "data": data,
        "page": page,
        "total_pages": total_pages,
    }

    if page < total_pages:
        next_page = page + 1

        scheme, netloc, path, _, _ = urlsplit(base_url)
        next_page = urlunsplit([scheme, netloc, path, f"page={next_page}", ""])
        response["next_page"] = next_page

    return json.dumps(response)


def generate_posts(count=100):
    return [{"id": i, "title": f"Post {i}"} for i in range(count)]


def generate_comments(post_id, count=50):
    return [{"id": i, "body": f"Comment {i} for post {post_id}"} for i in range(count)]


def paginated_callback(request, context, base_data, base_url):
    page = int(request.qs.get("page", [1])[0])
    page_size = 10
    total_items = len(base_data)
    total_pages = (total_items + page_size - 1) // page_size
    start_index = (page - 1) * 10
    end_index = start_index + 10
    data = base_data[start_index:end_index]
    return generate_paginated_response(data, page, total_pages, base_url)


@pytest.fixture(scope="module")
def mock_api_server():
    with requests_mock.Mocker() as m:
        # Posts data
        m.get(
            re.compile(r"https://api\.example\.com/posts(\?page=\d+)?$"),
            text=lambda request, context: paginated_callback(
                request, context, generate_posts(), f"{MOCK_BASE_URL}/posts"
            ),
        )

        # Comments data for each post
        m.get(
            re.compile(r"https://api\.example\.com/posts/(\d+)/comments"),
            text=lambda request, context: paginated_callback(
                request,
                context,
                generate_comments(int(request.url.split("/")[-2])),
                request.url,
            ),
        )

        # Mock the detail endpoint
        def detail_callback(request, context):
            post_id = request.url.split("/")[-1]
            return json.dumps({"id": post_id, "body": f"Post body {post_id}"})

        m.get(re.compile(r"https://api\.example\.com/posts/\d+$"), text=detail_callback)

        yield m
