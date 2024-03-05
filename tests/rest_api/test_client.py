import os
import pytest
from sources.rest_api.client import RESTClient
from sources.rest_api.paginators import JSONResponsePaginator
from sources.rest_api.auth import BearerTokenAuth, APIKeyAuth, OAuthJWTAuth


def load_private_key(name="private_key.pem"):
    key_path = os.path.join(os.path.dirname(__file__), name)
    with open(key_path, "r") as key_file:
        return key_file.read()


TEST_PRIVATE_KEY = load_private_key()


@pytest.fixture
def rest_client() -> RESTClient:
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

    def test_pagination(self, rest_client: RESTClient):
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

    def test_paginate_with_response_actions(self, rest_client: RESTClient):
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

    def test_basic_auth_success(self, rest_client: RESTClient):
        response = rest_client.get(
            "/protected/posts/basic-auth",
            auth=("user", "password"),
        )
        assert response.status_code == 200
        assert response.json()["data"][0] == {"id": 0, "title": "Post 0"}

        pages_iter = rest_client.paginate(
            "/protected/posts/basic-auth",
            auth=("user", "password"),
        )

        pages = list(pages_iter)
        self._assert_pagination(pages)

    def test_bearer_token_auth_success(self, rest_client: RESTClient):
        response = rest_client.get(
            "/protected/posts/bearer-token",
            auth=BearerTokenAuth("test-token"),
        )
        assert response.status_code == 200
        assert response.json()["data"][0] == {"id": 0, "title": "Post 0"}

        pages_iter = rest_client.paginate(
            "/protected/posts/bearer-token",
            auth=BearerTokenAuth("test-token"),
        )

        pages = list(pages_iter)
        self._assert_pagination(pages)

    def test_api_key_auth_success(self, rest_client: RESTClient):
        response = rest_client.get(
            "/protected/posts/api-key",
            auth=APIKeyAuth(key="x-api-key", value="test-api-key"),
        )
        assert response.status_code == 200
        assert response.json()["data"][0] == {"id": 0, "title": "Post 0"}

    def test_oauth_jwt_auth_success(self, rest_client: RESTClient):
        auth = OAuthJWTAuth(
            client_id="test-client-id",
            private_key=TEST_PRIVATE_KEY,
            auth_endpoint="https://api.example.com/oauth/token",
            scopes=["read", "write"],
            headers={"Content-Type": "application/json"},
        )

        response = rest_client.get(
            "/protected/posts/bearer-token",
            auth=auth,
        )

        assert response.status_code == 200
        assert "test-token" in response.request.headers["Authorization"]

        pages_iter = rest_client.paginate(
            "/protected/posts/bearer-token",
            auth=auth,
        )

        self._assert_pagination(list(pages_iter))