"""A source loading entities and lists from Affinity CRM (affinity.co)"""

from dataclasses import field
import typing as t
from typing import Any, Dict, Generator, Iterable, List, Sequence, Union

import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource
from dlt.extract.items import DataItemWithMeta


from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.client import RESTClient, Response
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
from dlt.common.logger import log_level, is_logging

from pydantic import ConfigDict, TypeAdapter

from .model import *
from .helpers import ListReference, generate_list_entries_path
from .settings import API_BASE, V2_PREFIX

import logging

if is_logging():
    # Configure the root logger
    logging.basicConfig(
        level=logging.DEBUG,  # Set the global logging level to DEBUG
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Log format
    )

    logging.getLogger("urllib3").setLevel(log_level())

LISTS_LITERAL = Literal['lists']
ENTITY = Literal['companies', 'persons', 'opportunities']
MAX_PAGE_LIMIT = 100

def get_entity_data_class(entity: ENTITY | LISTS_LITERAL):
    match entity:
        case "companies":
            return Company
        case "persons":
            return Person
        case "opportunities":
            return Opportunity
        case "lists":
            return ListModel


def get_entity_data_class_paged(entity: ENTITY):
    match entity:
        case "companies":
            return CompanyPaged
        case "persons":
            return PersonPaged
        case "opportunities":
            return OpportunityPaged


def __create_id_resource(entity: ENTITY | LISTS_LITERAL, is_id_generator: bool = True) -> DltResource:
    name = f"{entity}_ids" if is_id_generator else entity
    datacls = get_entity_data_class(entity)

    @dlt.resource(
        write_disposition="replace",
        primary_key="id",
        # can't use this yet, due to https://github.com/dlt-hub/dlt/pull/2109
        # columns=datacls,
        name=name,
        parallelized=True
    )
    def __ids(
        api_key: str = dlt.secrets["affinity_api_key"],
    ) -> Iterable[TDataItem]:
        rest_client = get_v2_rest_client(api_key)
        list_adapter = TypeAdapter(list[datacls])

        yield from (
            list_adapter.validate_python(entities)
            for entities in rest_client.paginate(entity, params={
                "limit": MAX_PAGE_LIMIT
            })
        )

    #__ids.add_limit(5)
    __ids.__name__ = name
    __ids.__qualname__ = name
    return __ids


def get_v2_rest_client(api_key: str):
    return RESTClient(
        base_url=f"{API_BASE}{V2_PREFIX}",
        auth=BearerTokenAuth(api_key),
        data_selector="data",
        paginator=JSONLinkPaginator("pagination.nextUrl"),
    )


@dlt.source(name="affinity")
def source(
    list_refs: List[ListReference] = field(default_factory=list)
) -> Sequence[DltResource]:

    list_resources = [__create_list_entries_resource(ref) for ref in list_refs]

    return (
        companies,
        persons,
        opportunities,
        lists,
        *list_resources,
    )

def mark_dropdown_item(dropdown_item: Dropdown | RankedDropdown, field: FieldModel) -> DataItemWithMeta:
    return dlt.mark.with_hints(
                            item={ "id": dropdown_item.dropdownOptionId } | dropdown_item.model_dump(exclude={"dropdownOptionId"}),
                            hints=dlt.mark.make_hints(
                                table_name=f"dropdown_options_{field.id}",
                                write_disposition="merge",
                                primary_key="id",
                                merge_key="id",
                                # can't use this yet, due to https://github.com/dlt-hub/dlt/pull/2109
                                # columns={
                                #     "id": {
                                #         "primary_key": True,
                                #         "unique": True,

                                #     },
                                #     "text": {
                                #         "data_type": "text"
                                #     }
                                # }
                            ),
                        )


def process_and_yield_fields(entity: Company | Person | OpportunityWithFields, ret: Dict[str, Any]) -> Generator[DataItemWithMeta, None, None]:
    if not entity.fields:
        return
    for field in entity.fields:
        yield dlt.mark.with_hints(
            item=field.model_dump(exclude={"value"}) | { "value_type": field.value.root.type },
            hints=dlt.mark.make_hints(
                table_name=f"fields",
                write_disposition="merge",
                primary_key="id",
                merge_key="id",
            )
        )
        new_column = f"{field.id}_{field.name}" if field.id.startswith("field-") else field.id
        value = field.value.root
        match value:
            case DateValue():
                ret[new_column] = value.data
            case DropdownValue() | RankedDropdownValue():
                ret[f"{new_column}_dropdown_option_id"] = value.data.dropdownOptionId if value.data is not None else None
                if value.data is not None:
                    yield mark_dropdown_item(value.data, field)
            case DropdownsValue():
                if value.data is None or len(value.data) == 0:
                    ret[new_column] = []
                    continue
                ret[new_column] = value.data
                for d in value.data:
                    yield mark_dropdown_item(d, field)
            case FormulaValue():
                ret[new_column] = value.data.calculatedValue
                raise ValueError(f"Value type {value} not implemented")
            case InteractionValue():
                if value.data is None:
                    ret[new_column] = None
                    continue
                interaction = value.data.root
                ret[new_column] = interaction.model_dump(include={"id", "type"})
                yield dlt.mark.with_hints(
                    item=value.data,
                    hints=dlt.mark.make_hints(
                        table_name=f"interactions_{interaction.type}",
                        write_disposition="merge",
                        primary_key="id",
                    ),
                )
            case PersonValue() | CompanyValue():
                ret[new_column] = value.data.id if value.data else None
            case PersonsValue() | CompaniesValue():
                ret[f"{new_column}_id"] = [e.id for e in value.data]
            case TextValue() | FloatValue() | TextValue() | TextsValue() | FloatsValue() | LocationValue() | LocationsValue():
                ret[new_column] = value.data
            case _:
                raise ValueError(f"Value type {value} not implemented")


def __create_entity_resource(entity_name: ENTITY) -> DltResource:
    datacls = get_entity_data_class_paged(entity_name)
    name = entity_name
    @dlt.transformer(
        # we fetch IDs for all entities first,
        # without any data, so we can parallelize the more expensive data fetching
        # whilst not hitting the API limits so fast and we can parallelize
        # because we don't need to page with cursors
        data_from=__create_id_resource(entity_name),
        write_disposition="replace",
        parallelized=True,
        primary_key="id",
        merge_key="id",
        max_table_nesting=3,
        #table_name=entity
        name=name
    )
    def __entities(
        entity_arr: t.List[Company | Person | Opportunity],
        api_key: str = dlt.secrets["affinity_api_key"],
    ) -> Iterable[TDataItem]:

        rest_client = get_v2_rest_client(api_key)
        ids = [x["id"] for x in entity_arr]
        response = rest_client.get(entity_name, params={
            # "limit": len(ids),
            "ids": ids,
            "fieldTypes": [Type2.ENRICHED.value, Type2.GLOBAL_.value, Type2.RELATIONSHIP_INTELLIGENCE.value]
        })
        response.raise_for_status()
        entities = datacls.model_validate_json(json_data=response.text)

        for e in entities.data:
            ret: Dict[str, Any] = {}
            yield from process_and_yield_fields(e, ret)
            yield dlt.mark.with_table_name(e.model_dump(exclude={"fields"}) | ret, name)

    __entities.__name__ = name
    __entities.__qualname__ = name
    return __entities


companies = __create_entity_resource('companies')
persons = __create_entity_resource('persons')
opportunities = __create_id_resource("opportunities", False)
lists = __create_id_resource("lists", False)



ErrorUnion = Field(
    Union[
        BadRequestError,
        ConflictError,
        MethodNotAllowedError,
        NotAcceptableError,
        NotImplementedError,
        RateLimitError,
        ServerError,
        UnprocessableEntityError,
        UnsupportedMediaTypeError,
    ],
    discriminator="code"
)

# TODO: other errors?
class Errors(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)  # Allow arbitrary types
    errors: List[ErrorUnion]  # type: ignore

def raise_if_error(response: Response, *args: Any, **kwargs: Any) -> None:
    if response.status_code < 200 or response.status_code >= 300:
        error_adapter = TypeAdapter(AuthenticationErrors | NotFoundErrors | AuthorizationErrors | ValidationErrors | Errors)
        error = error_adapter.validate_json(response.text)
        response.reason = "\n".join([e.message for e in error.errors])
        response.raise_for_status()

hooks = {
    "response": [raise_if_error]
}


def __create_list_entries_resource(list_ref: ListReference):
    name = f"lists-{list_ref}-entries"
    endpoint = generate_list_entries_path(list_ref)
    @dlt.resource(
        write_disposition="replace",
        parallelized=True,
        primary_key="id",
        merge_key="id",
        max_table_nesting=3,
        name=name
    )
    def __list_entries(
        api_key: str = dlt.secrets["affinity_api_key"],
    ) -> Iterable[TDataItem]:

        rest_client = get_v2_rest_client(api_key)
        list_adapter = TypeAdapter(list[ListEntryWithEntity])

        for list_entries in (
            list_adapter.validate_python(entities)
            for entities in rest_client.paginate(endpoint, params={
                "limit": MAX_PAGE_LIMIT,
                "fieldTypes": [Type2.ENRICHED.value, Type2.GLOBAL_.value, Type2.RELATIONSHIP_INTELLIGENCE.value, Type2.LIST.value]
            }, hooks=hooks)
        ):
            for list_entry in list_entries:
                e = list_entry.root
                ret: Dict[str, Any] = {
                    "entity_id": e.entity.id
                }
                yield from process_and_yield_fields(e.entity, ret)
                yield dlt.mark.with_table_name(e.model_dump(exclude={"entity"}) | ret, name)

    __list_entries.__name__ = name
    __list_entries.__qualname__ = name
    return __list_entries
