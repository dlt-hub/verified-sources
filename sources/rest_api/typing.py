from typing import (
    Any,
    Dict,
    List,
    NamedTuple,
    Optional,
    TypedDict,
    Union,
)

from dlt.sources.helpers.requests.retry import Client
from dlt.extract.typing import TTableHintTemplate
from dlt.extract.incremental import Incremental

from .paginators import BasePaginator


from dlt.common.schema.typing import (
    TColumnNames,
    # TSchemaContract,
    TTableFormat,
    TTableSchemaColumns,
    TWriteDisposition,
)

PaginatorConfigDict = Dict[str, Any]
PaginatorType = Union[Any, BasePaginator, str, PaginatorConfigDict]


class AuthConfig(TypedDict, total=False):
    token: str


class ClientConfig(TypedDict, total=False):
    base_url: str
    auth: Optional[Union[Any, AuthConfig]]
    paginator: Optional[PaginatorType]
    request_client: Optional[Client]


class IncrementalConfig(TypedDict, total=False):
    cursor_path: str
    initial_value: str
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
    method: Optional[str]
    params: Optional[Dict[str, Any]]
    json: Optional[Dict[str, Any]]
    paginator: Optional[PaginatorType]
    data_selector: Optional[Union[str, List[str]]]
    response_actions: Optional[List[ResponseAction]]


# TODO: check why validate_dict does not respect total=False
class EndpointResource(TypedDict, total=False):
    name: TTableHintTemplate[str]
    endpoint: Optional[Union[str, Endpoint]]
    write_disposition: Optional[TTableHintTemplate[TWriteDisposition]]
    parent: Optional[TTableHintTemplate[str]]
    columns: Optional[TTableHintTemplate[TTableSchemaColumns]]
    primary_key: Optional[TTableHintTemplate[TColumnNames]]
    merge_key: Optional[TTableHintTemplate[TColumnNames]]
    incremental: Optional[Incremental[Any]]
    table_format: Optional[TTableHintTemplate[TTableFormat]]
    include_from_parent: Optional[List[str]]


class FlexibleEndpointResource(EndpointResource, total=False):
    name: Optional[TTableHintTemplate[str]]


class RESTAPIConfig(TypedDict):
    client: ClientConfig
    resource_defaults: Optional[FlexibleEndpointResource]
    resources: List[Union[str, EndpointResource]]
