import pytest
from copy import copy, deepcopy
from typing import get_args

import dlt
from dlt.common.utils import update_dict_nested, custom_environ
from dlt.common.jsonpath import compile_path
from dlt.common.configuration import inject_section
from dlt.common.configuration.specs import ConfigSectionContext
from dlt.sources.helpers.rest_client.paginators import (
    SinglePagePaginator,
    HeaderLinkPaginator,
)

from sources.rest_api import rest_api_source, rest_api_resources
from sources.rest_api.config_setup import (
    AUTH_MAP,
    PAGINATOR_MAP,
    _bind_path_params,
    _setup_single_entity_endpoint,
    create_auth,
    create_paginator,
    _make_endpoint_resource,
    process_parent_data_item,
)
from sources.rest_api.typing import (
    AuthType,
    AuthTypeConfig,
    EndpointResource,
    PaginatorType,
    PaginatorTypeConfig,
    RESTAPIConfig,
    ResolvedParam,
)
from dlt.sources.helpers.rest_client.paginators import (
    HeaderLinkPaginator,
    JSONResponsePaginator,
    JSONResponseCursorPaginator,
    OffsetPaginator,
    PageNumberPaginator,
)
from dlt.sources.helpers.rest_client.auth import (
    AuthConfigBase,
    HttpBasicAuth,
    BearerTokenAuth,
    APIKeyAuth,
    OAuthJWTAuth,
)

from .source_configs import (
    AUTH_TYPE_CONFIGS,
    PAGINATOR_TYPE_CONFIGS,
    VALID_CONFIGS,
    INVALID_CONFIGS,
)


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
@pytest.mark.parametrize(
    "section", ("SOURCES__REST_API__CREDENTIALS", "SOURCES__CREDENTIALS", "CREDENTIALS")
)
def test_auth_shorthands(auth_type: AuthType, section: str) -> None:
    # mock all required envs
    with custom_environ(
        {
            f"{section}__TOKEN": "token",
            f"{section}__API_KEY": "api_key",
            f"{section}__USERNAME": "username",
            f"{section}__PASSWORD": "password",
        }
    ):
        # shorthands need to instantiate from config
        with inject_section(
            ConfigSectionContext(sections=("sources", "rest_api")), merge_existing=False
        ):
            auth = create_auth(auth_type)
            assert isinstance(auth, AUTH_MAP[auth_type])
            if isinstance(auth, BearerTokenAuth):
                assert auth.token == "token"
            if isinstance(auth, APIKeyAuth):
                assert auth.api_key == "api_key"
                assert auth.location == "header"
                assert auth.name == "Authorization"
            if isinstance(auth, HttpBasicAuth):
                assert auth.username == "username"
                assert auth.password == "password"


@pytest.mark.parametrize("auth_type_config", AUTH_TYPE_CONFIGS)
@pytest.mark.parametrize(
    "section", ("SOURCES__REST_API__CREDENTIALS", "SOURCES__CREDENTIALS", "CREDENTIALS")
)
def test_auth_type_configs(auth_type_config: AuthTypeConfig, section: str) -> None:
    # mock all required envs
    with custom_environ(
        {
            f"{section}__API_KEY": "api_key",
            f"{section}__NAME": "session-cookie",
            f"{section}__PASSWORD": "password",
        }
    ):
        # shorthands need to instantiate from config
        with inject_section(
            ConfigSectionContext(sections=("sources", "rest_api")), merge_existing=False
        ):
            auth = create_auth(auth_type_config)
            assert isinstance(auth, AUTH_MAP[auth_type_config["type"]])
            if isinstance(auth, BearerTokenAuth):
                # from typed dict
                assert auth.token == "token"
            if isinstance(auth, APIKeyAuth):
                assert auth.location == "cookie"
                # injected
                assert auth.api_key == "api_key"
                assert auth.name == "session-cookie"
            if isinstance(auth, HttpBasicAuth):
                # typed dict
                assert auth.username == "username"
                # injected
                assert auth.password == "password"


@pytest.mark.parametrize(
    "section", ("SOURCES__REST_API__CREDENTIALS", "SOURCES__CREDENTIALS", "CREDENTIALS")
)
def test_auth_instance_config(section: str) -> None:
    auth = APIKeyAuth(location="param", name="token")
    with custom_environ(
        {
            f"{section}__API_KEY": "api_key",
            f"{section}__NAME": "session-cookie",
        }
    ):
        # shorthands need to instantiate from config
        with inject_section(
            ConfigSectionContext(sections=("sources", "rest_api")), merge_existing=False
        ):
            # this also resolved configuration
            resolved_auth = create_auth(auth)
            assert resolved_auth is auth
            # explicit
            assert auth.location == "param"
            # injected
            assert auth.api_key == "api_key"
            # config overrides explicit (TODO: reverse)
            assert auth.name == "session-cookie"


def test_bearer_token_fallback() -> None:
    auth = create_auth({"token": "secret"})
    assert isinstance(auth, BearerTokenAuth)
    assert auth.token == "secret"


def test_resource_expand() -> None:
    # convert str into name / path
    assert _make_endpoint_resource("path", {}) == {
        "name": "path",
        "endpoint": {"path": "path"},
    }
    # expand endpoint str into path
    assert _make_endpoint_resource({"name": "resource", "endpoint": "path"}, {}) == {
        "name": "resource",
        "endpoint": {"path": "path"},
    }
    # expand name into path with optional endpoint
    assert _make_endpoint_resource({"name": "resource"}, {}) == {
        "name": "resource",
        "endpoint": {"path": "resource"},
    }
    # endpoint path is optional
    assert _make_endpoint_resource({"name": "resource", "endpoint": {}}, {}) == {
        "name": "resource",
        "endpoint": {"path": "resource"},
    }


def test_resource_endpoint_deep_merge() -> None:
    # columns deep merged
    resource = _make_endpoint_resource(
        {
            "name": "resources",
            "columns": [
                {"name": "col_a", "data_type": "bigint"},
                {"name": "col_b"},
            ],
        },
        {
            "columns": [
                {"name": "col_a", "data_type": "text", "primary_key": True},
                {"name": "col_c", "data_type": "timestamp", "partition": True},
            ]
        },
    )
    assert resource["columns"] == {
        # data_type and primary_key merged
        "col_a": {"name": "col_a", "data_type": "bigint", "primary_key": True},
        # from defaults
        "col_c": {"name": "col_c", "data_type": "timestamp", "partition": True},
        # from resource (partial column moved to the end)
        "col_b": {"name": "col_b"},
    }
    # json and params deep merged
    resource = _make_endpoint_resource(
        {
            "name": "resources",
            "endpoint": {
                "json": {"param1": "A", "param2": "B"},
                "params": {"param1": "A", "param2": "B"},
            },
        },
        {
            "endpoint": {
                "json": {"param1": "X", "param3": "Y"},
                "params": {"param1": "X", "param3": "Y"},
            }
        },
    )
    assert resource["endpoint"] == {
        "json": {"param1": "A", "param3": "Y", "param2": "B"},
        "params": {"param1": "A", "param3": "Y", "param2": "B"},
        "path": "resources",
    }


def test_resource_endpoint_shallow_merge() -> None:
    # merge paginators and other typed dicts as whole
    resource_config = {
        "name": "resources",
        "max_table_nesting": 5,
        "write_disposition": {"disposition": "merge", "x-merge-strategy": "scd2"},
        "schema_contract": {"tables": "freeze"},
        "endpoint": {
            "paginator": {"type": "cursor", "cursor_param": "cursor"},
            "incremental": {"cursor_path": "$", "start_param": "since"},
        },
    }

    resource = _make_endpoint_resource(
        resource_config,
        {
            "max_table_nesting": 1,
            "parallel": True,
            "write_disposition": {
                "disposition": "replace",
            },
            "schema_contract": {"columns": "freeze"},
            "endpoint": {
                "paginator": {
                    "type": "header_link",
                },
                "incremental": {
                    "cursor_path": "response.id",
                    "start_param": "since",
                    "end_param": "before",
                },
            },
        },
    )
    # resource should keep all values, just parallel is added
    expected_resource = copy(resource_config)
    expected_resource["parallel"] = True
    assert resource == expected_resource


def test_resource_merge_with_objects() -> None:
    paginator = SinglePagePaginator()
    incremental = dlt.sources.incremental[int]("id", row_order="asc")
    resource = _make_endpoint_resource(
        {
            "name": "resource",
            "endpoint": {
                "path": "path/to",
                "paginator": paginator,
                "params": {"since": incremental},
            },
        },
        {
            "table_name": lambda item: item["type"],
            "endpoint": {
                "paginator": HeaderLinkPaginator(),
                "params": {
                    "since": dlt.sources.incremental[int]("id", row_order="desc")
                },
            },
        },
    )
    # objects are as is, not cloned
    assert resource["endpoint"]["paginator"] is paginator
    assert resource["endpoint"]["params"]["since"] is incremental
    # callable coming from default
    assert callable(resource["table_name"])


def test_resource_merge_with_none() -> None:
    endpoint_config = {
        "name": "resource",
        "endpoint": {"path": "user/{id}", "paginator": None, "data_selector": None},
    }
    # None should be able to reset the default
    resource = _make_endpoint_resource(
        endpoint_config,
        {"endpoint": {"paginator": SinglePagePaginator(), "data_selector": "data"}},
    )
    # nones will overwrite defaults
    assert resource == endpoint_config


def test_setup_for_single_item_endpoint() -> None:
    # single item should revert to single page validator
    endpoint = _setup_single_entity_endpoint({"path": "user/{id}"})
    assert endpoint["data_selector"] == "$"
    assert isinstance(endpoint["paginator"], SinglePagePaginator)

    # this is not single page
    endpoint = _setup_single_entity_endpoint({"path": "user/{id}/messages"})
    assert "data_selector" not in endpoint

    # simulate using None to remove defaults
    endpoint_config = {
        "name": "resource",
        "endpoint": {"path": "user/{id}", "paginator": None, "data_selector": None},
    }
    # None should be able to reset the default
    resource = _make_endpoint_resource(
        endpoint_config,
        {"endpoint": {"paginator": HeaderLinkPaginator(), "data_selector": "data"}},
    )
    endpoint = _setup_single_entity_endpoint(resource["endpoint"])
    assert endpoint["data_selector"] == "$"
    assert isinstance(endpoint["paginator"], SinglePagePaginator)


def test_bind_path_param() -> None:
    three_params: EndpointResource = {
        "name": "comments",
        "endpoint": {
            "path": "{org}/{repo}/issues/{id}/comments",
            "params": {
                "org": "dlt-hub",
                "repo": "dlt",
                "id": {
                    "type": "resolve",
                    "field": "id",
                    "resource": "issues",
                },
            },
        },
    }
    tp_1 = deepcopy(three_params)
    _bind_path_params(tp_1)
    # do not replace resolved params
    assert tp_1["endpoint"]["path"] == "dlt-hub/dlt/issues/{id}/comments"
    # bound params popped
    assert len(tp_1["endpoint"]["params"]) == 1
    assert "id" in tp_1["endpoint"]["params"]

    tp_2 = deepcopy(three_params)
    tp_2["endpoint"]["params"]["id"] = 12345
    _bind_path_params(tp_2)
    assert tp_2["endpoint"]["path"] == "dlt-hub/dlt/issues/12345/comments"
    assert len(tp_2["endpoint"]["params"]) == 0

    # param missing
    tp_3 = deepcopy(three_params)
    with pytest.raises(ValueError) as val_ex:
        del tp_3["endpoint"]["params"]["id"]
        _bind_path_params(tp_3)
    # path is a part of an exception
    assert tp_3["endpoint"]["path"] in str(val_ex.value)

    # path without params
    tp_4 = deepcopy(three_params)
    tp_4["endpoint"]["path"] = "comments"
    # no unbound params
    del tp_4["endpoint"]["params"]["id"]
    tp_5 = deepcopy(tp_4)
    _bind_path_params(tp_4)
    assert tp_4 == tp_5

    # resolved param will remain unbounded and
    tp_6 = deepcopy(three_params)
    tp_6["endpoint"]["path"] = "{org}/{repo}/issues/1234/comments"
    with pytest.raises(NotImplementedError):
        _bind_path_params(tp_6)


def test_process_parent_data_item():
    resolve_param = ResolvedParam(
        "id", {"field": "obj_id", "resource": "issues", "type": "resolve"}
    )
    bound_path, parent_record = process_parent_data_item(
        "dlt-hub/dlt/issues/{id}/comments", {"obj_id": 12345}, resolve_param, None
    )
    assert bound_path == "dlt-hub/dlt/issues/12345/comments"
    assert parent_record == {}

    bound_path, parent_record = process_parent_data_item(
        "dlt-hub/dlt/issues/{id}/comments", {"obj_id": 12345}, resolve_param, ["obj_id"]
    )
    assert parent_record == {"_issues_obj_id": 12345}

    bound_path, parent_record = process_parent_data_item(
        "dlt-hub/dlt/issues/{id}/comments",
        {"obj_id": 12345, "obj_node": "node_1"},
        resolve_param,
        ["obj_id", "obj_node"],
    )
    assert parent_record == {"_issues_obj_id": 12345, "_issues_obj_node": "node_1"}

    # param path not found
    with pytest.raises(ValueError) as val_ex:
        bound_path, parent_record = process_parent_data_item(
            "dlt-hub/dlt/issues/{id}/comments", {"_id": 12345}, resolve_param, None
        )
    assert "Transformer expects a field 'obj_id'" in str(val_ex.value)

    # included path not found
    with pytest.raises(ValueError) as val_ex:
        bound_path, parent_record = process_parent_data_item(
            "dlt-hub/dlt/issues/{id}/comments",
            {"obj_id": 12345, "obj_node": "node_1"},
            resolve_param,
            ["obj_id", "node"],
        )
    assert "in order to include it in child records under _issues_node" in str(
        val_ex.value
    )


def test_resource_schema() -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            "users",
            {
                "name": "user",
                "endpoint": {
                    "path": "user/{id}",
                    "paginator": None,
                    "data_selector": None,
                    "params": {
                        "id": {
                            "type": "resolve",
                            "field": "id",
                            "resource": "users",
                        },
                    },
                },
            },
        ],
    }
    resources = rest_api_resources(config)
    assert len(resources) == 2
    resource = resources[0]
    assert resource.name == "users"
    assert resources[1].name == "user"
