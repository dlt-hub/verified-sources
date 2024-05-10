import pytest
from typing import get_args

from dlt.common.utils import update_dict_nested, custom_environ
from dlt.common.jsonpath import compile_path
from dlt.common.configuration import inject_section
from dlt.common.configuration.specs import ConfigSectionContext

from sources.rest_api import rest_api_source
from sources.rest_api.config_setup import (
    AUTH_MAP,
    PAGINATOR_MAP,
    create_auth,
    create_paginator,
)
from sources.rest_api.typing import (
    AuthType,
    PaginatorType,
    PaginatorTypeConfig,
    RESTAPIConfig,
)
from dlt.sources.helpers.rest_client.paginators import (
    HeaderLinkPaginator,
    JSONResponsePaginator,
    JSONResponseCursorPaginator,
    OffsetPaginator,
    PageNumberPaginator,
)

from .source_configs import PAGINATOR_TYPE_CONFIGS, VALID_CONFIGS, INVALID_CONFIGS


@pytest.mark.parametrize("expected_message, exception, invalid_config", INVALID_CONFIGS)
def test_invalid_configurations(expected_message, exception, invalid_config):
    with pytest.raises(exception, match=expected_message):
        rest_api_source(invalid_config)


@pytest.mark.parametrize("valid_config", VALID_CONFIGS)
def test_valid_configurations(valid_config):
    rest_api_source(valid_config)


@pytest.mark.parametrize("config", VALID_CONFIGS)
def test_configurations_dict_is_not_modified_in_place(config):
    # deep clone dicts but do not touch instances of classes so ids still compare
    config_copy = update_dict_nested({}, config)
    rest_api_source(config)
    assert config_copy == config


@pytest.mark.parametrize("paginator_type", get_args(PaginatorType))
def test_paginator_shorthands(paginator_type: PaginatorType) -> None:
    # config: RESTAPIConfig = {
    #     "client": {
    #         "base_url": "https://api.example.com",
    #         "paginator": paginator_type
    #     },
    #     "resources": [
    #         "resource"
    #     ]
    # }
    try:
        create_paginator(paginator_type)
    except ValueError as v_ex:
        # offset paginator cannot be instantiated
        assert paginator_type == "offset"
        assert "offset" in str(v_ex)


@pytest.mark.parametrize("paginator_type_config", PAGINATOR_TYPE_CONFIGS)
def test_paginator_type_configs(paginator_type_config: PaginatorTypeConfig) -> None:
    paginator = create_paginator(paginator_type_config)
    if paginator_type_config["type"] == "auto":
        assert paginator is None
    else:
        # assert types and default params
        assert isinstance(paginator, PAGINATOR_MAP[paginator_type_config["type"]])
        # check if params are bound
        if isinstance(paginator, HeaderLinkPaginator):
            assert paginator.links_next_key == "next_page"
        if isinstance(paginator, PageNumberPaginator):
            assert paginator.current_value == 10
            assert paginator.param_name == "page"
            assert paginator.total_path == compile_path("response.pages")
            assert paginator.maximum_value is None
        if isinstance(paginator, OffsetPaginator):
            assert paginator.current_value == 0
            assert paginator.param_name == "offset"
            assert paginator.limit == 100
            assert paginator.limit_param == "limit"
            assert paginator.total_path == compile_path("total")
            assert paginator.maximum_value == 1000
        if isinstance(paginator, JSONResponsePaginator):
            assert paginator.next_url_path == compile_path("response.nex_page_link")
        if isinstance(paginator, JSONResponseCursorPaginator):
            assert paginator.cursor_path == compile_path("cursors.next")
            assert paginator.cursor_param == "cursor"


def test_paginator_instance_config() -> None:
    paginator = OffsetPaginator(limit=100)
    assert create_paginator(paginator) is paginator


@pytest.mark.parametrize("auth_type", get_args(AuthType))
def test_auth_shorthands(auth_type: AuthType) -> None:
    # mock all envs
    with custom_environ(
        {
            "SOURCES__REST_API__CREDENTIALS__TOKEN": "token",
            "SOURCES__REST_API__CREDENTIALS__API_KEY": "api_key",
            "SOURCES__REST_API__CREDENTIALS__USERNAME": "username",
            "SOURCES__REST_API__CREDENTIALS__PASSWORD": "password",
        }
    ):
        # shorthands need to instantiate from config
        with inject_section(
            ConfigSectionContext(sections=("sources", "rest_api")), merge_existing=False
        ):
            auth = create_auth(auth_type)
            assert isinstance(auth, AUTH_MAP[auth_type])
