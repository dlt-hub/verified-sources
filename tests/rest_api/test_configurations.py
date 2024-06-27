import dlt.extract
import pytest
from copy import copy, deepcopy
from typing import cast, get_args, Dict, List, Any

import dlt
from dlt.common.utils import update_dict_nested, custom_environ
from dlt.common.jsonpath import compile_path
from dlt.common.configuration import inject_section
from dlt.common.configuration.specs import ConfigSectionContext
from dlt.sources.helpers.rest_client.paginators import (
    SinglePagePaginator,
    HeaderLinkPaginator,
)

from sources.rest_api import rest_api_source, rest_api_resources, _validate_param_type
from sources.rest_api.config_setup import (
    AUTH_MAP,
    PAGINATOR_MAP,
    IncrementalParam,
    _bind_path_params,
    _setup_single_entity_endpoint,
    create_auth,
    create_paginator,
    _make_endpoint_resource,
    process_parent_data_item,
    setup_incremental_object,
    create_response_hooks,
    _handle_response_action,
)
from sources.rest_api.typing import (
    AuthType,
    AuthTypeConfig,
    EndpointResource,
    PaginatorType,
    PaginatorTypeConfig,
    RESTAPIConfig,
    ResolvedParam,
    ResponseAction,
    IncrementalConfig,
)
from dlt.sources.helpers.rest_client.paginators import (
    HeaderLinkPaginator,
    JSONResponsePaginator,
    JSONResponseCursorPaginator,
    OffsetPaginator,
    PageNumberPaginator,
)
from dlt.sources.helpers.rest_client.auth import (
    HttpBasicAuth,
    BearerTokenAuth,
    APIKeyAuth,
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


def test_error_message_invalid_auth_type() -> None:
    with pytest.raises(ValueError) as e:
        create_auth("non_existing_method")
    assert (
        str(e.value)
        == "Invalid authentication: non_existing_method. Available options: bearer, api_key, http_basic"
    )


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

    # test nested data
    resolve_param_nested = ResolvedParam(
        "id", {"field": "some_results.obj_id", "resource": "issues", "type": "resolve"}
    )
    item = {"some_results": {"obj_id": 12345}}
    bound_path, parent_record = process_parent_data_item(
        "dlt-hub/dlt/issues/{id}/comments", item, resolve_param_nested, None
    )
    assert bound_path == "dlt-hub/dlt/issues/12345/comments"

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


def test_incremental_from_request_param():
    request_params = {
        "foo": "bar",
        "since": {
            "type": "incremental",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-01T00:00:00Z",
        },
    }
    (incremental_config, incremental_param) = setup_incremental_object(request_params)
    assert incremental_config == dlt.sources.incremental(
        cursor_path="updated_at", initial_value="2024-01-01T00:00:00Z"
    )
    assert incremental_param == IncrementalParam(start="since", end=None)


def test_invalid_incremental():
    request_params = {
        "foo": "bar",
        "since": {
            "type": "no_incremental",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-01T00:00:00Z",
        },
    }
    with pytest.raises(ValueError) as e:
        _validate_param_type(request_params)

    assert e.match("Invalid param type: no_incremental.")


def test_incremental_source_in_request_param():
    request_params = {
        "foo": "bar",
        "since": dlt.sources.incremental(
            cursor_path="updated_at", initial_value="2024-01-01T00:00:00Z"
        ),
    }
    (incremental_config, incremental_param) = setup_incremental_object(request_params)
    assert incremental_config == dlt.sources.incremental(
        cursor_path="updated_at", initial_value="2024-01-01T00:00:00Z"
    )
    assert incremental_param == IncrementalParam(start="since", end=None)


def test_endpoint_config_incremental():
    config = {
        "incremental": {
            "start_param": "since",
            "end_param": "until",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-25T11:21:28Z",
        }
    }
    incremental_config = cast(IncrementalConfig, config.get("incremental"))
    (_, incremental_param) = setup_incremental_object(
        {},
        incremental_config,
    )
    assert incremental_param == IncrementalParam(start="since", end="until")


def test_many_incrementals_in_resource():
    request_params = {
        "foo": "bar",
        "first_incremental": {
            "type": "incremental",
            "cursor_path": "updated_at",
            "initial_value": "2024-01-01T00:00:00Z",
        },
        "second_incremental": {
            "type": "incremental",
            "cursor_path": "created_at",
            "initial_value": "2024-01-01T00:00:00Z",
        },
    }
    with pytest.raises(ValueError) as e:
        setup_incremental_object(request_params)

    assert (
        "Only a single incremental parameter is allower per endpoint. Found: ['first_incremental', 'second_incremental']"
        in str(e.value)
    )


def test_resource_hints():
    config: RESTAPIConfig = {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            {
                "name": "posts",
                "endpoint": {
                    "params": {
                        "limit": 100,
                    },
                },
                "table_name": "a_table",
                "max_table_nesting": 2,
                "write_disposition": "merge",
                "columns": {"a_text": {"name": "a_text", "data_type": "text"}},
                "primary_key": "a_pk",
                "merge_key": "a_merge_key",
                "schema_contract": {"tables": "evolve"},
                "table_format": "iceberg",
                "selected": False,
            },
        ],
    }

    resources = rest_api_resources(config)
    assert resources[0].name == "posts"
    assert resources[0].table_name == "a_table"
    assert resources[0].max_table_nesting == 2
    assert resources[0].write_disposition == "merge"
    assert resources[0].columns == {"a_text": {"name": "a_text", "data_type": "text"}}
    schema = resources[0].compute_table_schema()
    primary_keys = [
        spec["name"]
        for _, spec in schema["columns"].items()
        if spec.get("primary_key", False)
    ]
    assert primary_keys == ["a_pk"]
    merge_keys = [
        spec["name"]
        for _, spec in schema["columns"].items()
        if spec.get("merge_key", False)
    ]
    assert merge_keys == ["a_merge_key"]
    assert resources[0].schema_contract == {"tables": "evolve"}
    assert schema.get("table_format") == "iceberg"
    assert resources[0].selected is False
    # TODO: test if it is parallelized and has spec


def test_create_multiple_response_actions():
    def custom_hook(response, *args, **kwargs):
        return response

    response_actions: List[ResponseAction] = [
        custom_hook,
        {"status_code": 404, "action": "ignore"},
        {"content": "Not found", "action": "ignore"},
        {"status_code": 200, "content": "some text", "action": "ignore"},
    ]
    hooks = cast(Dict[str, Any], create_response_hooks(response_actions))
    assert len(hooks["response"]) == 4

    response_actions_2: List[ResponseAction] = [
        custom_hook,
        {"status_code": 200, "action": custom_hook},
    ]
    hooks_2 = cast(Dict[str, Any], create_response_hooks(response_actions_2))
    assert len(hooks_2["response"]) == 2


def test_response_action_raises_type_error(mocker):
    class C:
        pass

    response = mocker.Mock()
    response.status_code = 200

    with pytest.raises(ValueError) as e_1:
        _handle_response_action(response, {"status_code": 200, "action": C()})
    assert e_1.match("does not conform to expected type")

    with pytest.raises(ValueError) as e_2:
        _handle_response_action(response, {"status_code": 200, "action": 123})
    assert e_2.match("does not conform to expected type")

    assert ("ignore", None) == _handle_response_action(
        response, {"status_code": 200, "action": "ignore"}
    )
    assert ("foobar", None) == _handle_response_action(
        response, {"status_code": 200, "action": "foobar"}
    )


def test_parses_hooks_from_response_actions(mocker):
    response = mocker.Mock()
    response.status_code = 200

    hook_1 = mocker.Mock()
    hook_2 = mocker.Mock()

    assert (None, [hook_1]) == _handle_response_action(
        response, {"status_code": 200, "action": hook_1}
    )
    assert (None, [hook_1, hook_2]) == _handle_response_action(
        response, {"status_code": 200, "action": [hook_1, hook_2]}
    )
