import pytest
from sources import rest_api
from sources.rest_api.typing import PaginatorConfig
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator


class CustomPaginator(JSONLinkPaginator):
    """A paginator that uses a specific key in the JSON response to find
    the next page URL.
    """

    def __init__(
        self,
        next_url_path="$['@odata.nextLink']",
    ):
        super().__init__(next_url_path=next_url_path)


class TestCustomPaginator:
    @pytest.fixture
    def custom_paginator_config(self) -> PaginatorConfig:
        config: PaginatorConfig = {
            "type": "custom_paginator",  # type: ignore
            "next_url_path": "response.next_page_link",
        }
        return config

    def test_creates_builtin_paginator_without_registering(self) -> None:
        config: PaginatorConfig = {
            "type": "json_response",
            "next_url_path": "response.next_page_link",
        }
        paginator = rest_api.config_setup.create_paginator(config)
        assert paginator.has_next_page is True

    def test_not_registering_throws_error(self, custom_paginator_config) -> None:
        with pytest.raises(ValueError) as e:
            rest_api.config_setup.create_paginator(custom_paginator_config)

        assert e.match("Invalid paginator: custom_paginator.")

    def test_registering_adds_to_PAGINATOR_MAP(self, custom_paginator_config) -> None:
        rest_api.config_setup.register_paginator("custom_paginator", CustomPaginator)
        cls = rest_api.config_setup.get_paginator_class("custom_paginator")
        assert cls is CustomPaginator

        # teardown test
        del rest_api.config_setup.PAGINATOR_MAP["custom_paginator"]

    def test_registering_allows_usage(self, custom_paginator_config) -> None:
        rest_api.config_setup.register_paginator("custom_paginator", CustomPaginator)
        paginator = rest_api.config_setup.create_paginator(custom_paginator_config)
        assert paginator.has_next_page is True
        assert str(paginator.next_url_path) == "response.next_page_link"

        # teardown test
        del rest_api.config_setup.PAGINATOR_MAP["custom_paginator"]

    def test_registering_not_base_paginator_throws_error(self) -> None:
        class NotAPaginator:
            pass

        with pytest.raises(ValueError) as e:
            rest_api.config_setup.register_paginator("not_a_paginator", NotAPaginator)
        assert e.match("Invalid paginator: NotAPaginator.")
