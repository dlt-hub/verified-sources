from typing import (
    Any,
    Dict,
    List,
    NamedTuple,
    Optional,
    TypedDict,
    Union,
    Literal,
)

from dlt.common import jsonpath
from dlt.common.typing import TSortOrder
from dlt.extract.items import TTableHintTemplate
from dlt.extract.incremental.typing import LastValueFunc

from .paginators import BasePaginator
from .auth import AuthConfigBase

from dlt.common.schema.typing import (
    TColumnNames,
    # TSchemaContract,
    TTableFormat,
    TTableSchemaColumns,
    TWriteDisposition,
)

PaginatorConfigDict = Dict[str, Any]
PaginatorType = Union[BasePaginator, str, PaginatorConfigDict]

HTTPMethodBasic = Literal["GET", "POST"]
HTTPMethodExtended = Literal["PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]
HTTPMethod = Union[HTTPMethodBasic, HTTPMethodExtended]


class AuthConfig(TypedDict, total=False):
    token: str


class ClientConfig(TypedDict, total=False):
    base_url: str
    auth: Optional[Union[AuthConfig, AuthConfigBase]]
    paginator: Optional[PaginatorType]


class IncrementalArgs(TypedDict, total=False):
    cursor_path: str
    initial_value: Optional[str]
    last_value_func: LastValueFunc[str]
    primary_key: Optional[TTableHintTemplate[TColumnNames]]
    end_value: Optional[str]
    row_order: Optional[TSortOrder]


class IncrementalConfig(IncrementalArgs, total=False):
    param: str


class ResolveConfig(NamedTuple):
    resource_name: str
    field_path: str


class ResolvedParam(NamedTuple):
    param_name: str
    resolve_config: ResolveConfig


class ResponseAction(TypedDict, total=False):
    status_code: Optional[Union[int, str]]
    content: Optional[str]
    action: str


class Endpoint(TypedDict, total=False):
    path: Optional[str]
    method: Optional[HTTPMethodBasic]
    params: Optional[Dict[str, Any]]
    json: Optional[Dict[str, Any]]
    paginator: Optional[PaginatorType]
    data_selector: Optional[jsonpath.TJsonPath]
    response_actions: Optional[List[ResponseAction]]


class EndpointResourceBase(TypedDict, total=False):
    endpoint: Optional[Union[str, Endpoint]]
    write_disposition: Optional[TTableHintTemplate[TWriteDisposition]]
    parent: Optional[TTableHintTemplate[str]]
    columns: Optional[TTableHintTemplate[TTableSchemaColumns]]
    primary_key: Optional[TTableHintTemplate[TColumnNames]]
    merge_key: Optional[TTableHintTemplate[TColumnNames]]
    incremental: Optional[IncrementalConfig]
    table_format: Optional[TTableHintTemplate[TTableFormat]]
    include_from_parent: Optional[List[str]]
    selected: Optional[bool]


# NOTE: redefining properties of TypedDict is not allowed
class EndpointResource(EndpointResourceBase, total=False):
    name: TTableHintTemplate[str]


class DefaultEndpointResource(EndpointResourceBase, total=False):
    name: Optional[TTableHintTemplate[str]]


class RESTAPIConfig(TypedDict):
    client: ClientConfig
    resource_defaults: Optional[DefaultEndpointResource]
    resources: List[Union[str, EndpointResource]]
