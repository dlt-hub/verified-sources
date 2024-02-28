import pytest
from sources.rest_api.client import RESTClient
from sources.rest_api.paginators import JSONResponsePaginator


@pytest.fixture
def rest_client():
    return RESTClient(
        base_url="https://api.example.com",
        headers={"Accept": "application/json"},
    )


@pytest.mark.usefixtures("mock_api_server")
class TestRESTClient:
    def _assert_pagination(self, pages):
        for i, page in enumerate(pages):
            assert page == [
                {"id": i, "title": f"Post {i}"} for i in range(i * 10, (i + 1) * 10)
            ]

    def test_get_single_resource(self, rest_client):
        response = rest_client.get("/posts/1")
        assert response.status_code == 200
        assert response.json() == {"id": "1", "body": "Post body 1"}

    def test_pagination(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts",
            paginator=JSONResponsePaginator(next_key="next_page"),
        )

        pages = list(pages_iter)

        self._assert_pagination(pages)

    def test_default_paginator(self, rest_client):
        pages_iter = rest_client.paginate("/posts")

        pages = list(pages_iter)

        self._assert_pagination(pages)

    def test_paginate_with_response_actions(self, rest_client):
        pages_iter = rest_client.paginate(
            "/posts",
            paginator=JSONResponsePaginator(next_key="next_page"),
            response_actions=[
                {"status_code": 404, "action": "ignore"},
            ],
        )

        pages = list(pages_iter)

        self._assert_pagination(pages)

        pages_iter = rest_client.paginate(
            "/posts/1/some_details_404",
            paginator=JSONResponsePaginator(),
            response_actions=[
                {"status_code": 404, "action": "ignore"},
            ],
        )

        pages = list(pages_iter)
        assert pages == []
