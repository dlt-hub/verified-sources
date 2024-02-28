import pytest
import requests_mock
import re
import json
from urllib.parse import urlsplit, urlunsplit

MOCK_BASE_URL = "https://api.example.com"


class APIRouter:
    def __init__(self, base_url):
        self.routes = []
        self.base_url = base_url

    def get(self, pattern):
        def decorator(func):
            self.routes.append((re.compile(f"{self.base_url}{pattern}"), func))
            return func

        return decorator

    def register_routes(self, mocker):
        for pattern, callback in self.routes:
            mocker.register_uri("GET", pattern, text=callback)


router = APIRouter(MOCK_BASE_URL)


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


def paginate_response(request, context, base_data, base_url):
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

        @router.get("/posts(\?page=\d+)?$")
        def posts(request, context):
            return paginate_response(request, context, generate_posts(), request.url)

        @router.get("/posts/(\d+)/comments")
        def post_comments(request, context):
            post_id = int(request.url.split("/")[-2])
            return paginate_response(
                request, context, generate_comments(post_id), request.url
            )

        @router.get("/posts/\d+$")
        def post_detail(request, context):
            post_id = request.url.split("/")[-1]
            return json.dumps({"id": post_id, "body": f"Post body {post_id}"})

        @router.get("/posts/\d+/some_details_404")
        def post_detail_404(request, context):
            """Return 404 for post with id > 0. Used to test ignoring 404 errors.
            """
            post_id = int(request.url.split("/")[-2])
            if post_id < 1:
                return json.dumps({"id": post_id, "body": f"Post body {post_id}"})
            else:
                context.status_code = 404
                return json.dumps({"error": "Post not found"})

        router.register_routes(m)

        yield m
