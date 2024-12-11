"""A source loading player profiles and games from chess.com api"""

from dataclasses import field
import typing as t
from typing import Any, Dict, Generator, Iterable, List, Sequence, Union

import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource
from dlt.extract.items import DataItemWithMeta


from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.client import RESTClient, Response, raise_for_status
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

from pydantic import TypeAdapter

from .model import *
from .helpers import ListReference, generate_list_entries_path
from .settings import API_BASE, V2_PREFIX

#from ..rest_api import rest_api_resources

import logging

# Configure the root logger
logging.basicConfig(
    level=logging.DEBUG,  # Set the global logging level to DEBUG
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Log format
)

# Set urllib3 logging level to DEBUG
logging.getLogger("urllib3").setLevel(logging.DEBUG)

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
        # case "lists":
        #     return ListPaged


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

    __ids.add_limit(5)
    #__ids.add_map(lambda entity: {"id": entity.id })
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

# { dropdownOptionId: 111, text: ... }
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
        ids = [x.id for x in entity_arr]
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



# TODO: other errors
# class Error(BaseModel):
#     errors: List[BadRequestError | ConflictError |MethodNotAllowedError | NotAcceptableError | NotImplementedError | RateLimitError | ServerError | UnprocessableEntityError | UnsupportedMediaTypeError] = Field(discriminator="code")

def raise_if_error(response: Response, *args: Any, **kwargs: Any) -> None:
    if response.status_code < 200 or response.status_code >= 300:
        error_adapter = TypeAdapter(AuthenticationErrors | NotFoundErrors | AuthorizationErrors | ValidationErrors)
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

# @dlt.source(name="chess")
# def source(
#     players: List[str], start_month: str = None, end_month: str = None
# ) -> Sequence[DltResource]:
#     """
#     A dlt source for the chess.com api. It groups several resources (in this case chess.com API endpoints) containing
#     various types of data: user profiles or chess match results
#     Args:
#         players (List[str]): A list of the player usernames for which to get the data.
#         start_month (str, optional): Filters out all the matches happening before `start_month`. Defaults to None.
#         end_month (str, optional): Filters out all the matches happening after `end_month`. Defaults to None.
#     Returns:
#         Sequence[DltResource]: A sequence of resources that can be selected from including players_profiles,
#         players_archives, players_games, players_online_status
#     """
#     return (
#         players_profiles(players),
#         players_archives(players),
#         players_games(players, start_month=start_month, end_month=end_month),
#         players_online_status(players),
#     )


# @dlt.resource(
#     write_disposition="replace",
#     columns={
#         "last_online": {"data_type": "timestamp"},
#         "joined": {"data_type": "timestamp"},
#     },
# )
# def players_profiles(players: List[str]) -> Iterator[TDataItem]:
#     """
#     Yields player profiles for a list of player usernames.
#     Args:
#         players (List[str]): List of player usernames to retrieve profiles for.
#     Yields:
#         Iterator[TDataItem]: An iterator over player profiles data.
#     """

#     # get archives in parallel by decorating the http request with defer
#     @dlt.defer
#     def _get_profile(username: str) -> TDataItem:
#         return get_path_with_retry(f"player/{username}")

#     for username in players:
#         yield _get_profile(username)


# @dlt.resource(write_disposition="replace", selected=False)
# def players_archives(players: List[str]) -> Iterator[List[TDataItem]]:
#     """
#     Yields url to game archives for specified players.
#     Args:
#         players (List[str]): List of player usernames to retrieve archives for.
#     Yields:
#         Iterator[List[TDataItem]]: An iterator over list of player archive data.
#     """
#     for username in players:
#         data = get_path_with_retry(f"player/{username}/games/archives")
#         yield data.get("archives", [])


# @dlt.resource(
#     write_disposition="append", columns={"end_time": {"data_type": "timestamp"}}
# )
# def players_games(
#     players: List[str], start_month: str = None, end_month: str = None
# ) -> Iterator[Callable[[], List[TDataItem]]]:
#     """
#     Yields `players` games that happened between `start_month` and `end_month`.
#     Args:
#         players (List[str]): List of player usernames to retrieve games for.
#         start_month (str, optional): The starting month in the format "YYYY/MM". Defaults to None.
#         end_month (str, optional): The ending month in the format "YYYY/MM". Defaults to None.
#     Yields:
#         Iterator[Callable[[], List[TDataItem]]]: An iterator over callables that return a list of games for each player.
#     """  # do a simple validation to prevent common mistakes in month format
#     validate_month_string(start_month)
#     validate_month_string(end_month)

#     # get a list of already checked archives
#     # from your point of view, the state is python dictionary that will have the same content the next time this function is called
#     checked_archives = dlt.current.resource_state().setdefault("archives", [])
#     # get player archives, note that you can call the resource like any other function and just iterate it like a list
#     archives = players_archives(players)

#     # get archives in parallel by decorating the http request with defer
#     @dlt.defer
#     def _get_archive(url: str) -> List[TDataItem]:
#         print(f"Getting archive from {url}")
#         try:
#             games = get_url_with_retry(url).get("games", [])
#             return games  # type: ignore
#         except requests.HTTPError as http_err:
#             # sometimes archives are not available and the error seems to be permanent
#             if http_err.response.status_code == 404:
#                 return []
#             raise

#     # enumerate the archives
#     for url in archives:
#         # the `url` format is https://api.chess.com/pub/player/{username}/games/{YYYY}/{MM}
#         if start_month and url[-7:] < start_month:
#             continue
#         if end_month and url[-7:] > end_month:
#             continue
#         # do not download archive again
#         if url in checked_archives:
#             continue
#         checked_archives.append(url)
#         # get the filtered archive
#         yield _get_archive(url)


# @dlt.resource(write_disposition="append")
# def players_online_status(players: List[str]) -> Iterator[TDataItem]:
#     """
#     Returns current online status for a list of players.
#     Args:
#         players (List[str]): List of player usernames to check online status for.
#     Yields:
#         Iterator[TDataItem]: An iterator over the online status of each player.
#     """
#     # we'll use unofficial endpoint to get online status, the official seems to be removed
#     for player in players:
#         status = get_url_with_retry(f"{UNOFFICIAL_CHESS_API_URL}user/popup/{player}")
#         # return just relevant selection
#         yield {
#             "username": player,
#             "onlineStatus": status["onlineStatus"],
#             "lastLoginDate": status["lastLoginDate"],
#             "check_time": pendulum.now(),  # dlt can deal with native python dates
#         }


# @dlt.source
# def chess_dlt_config_example(
#     secret_str: str = dlt.secrets.value,
#     secret_dict: Dict[str, Any] = dlt.secrets.value,
#     config_int: int = dlt.config.value,
# ) -> DltResource:
#     """
#     An example of a source that uses dlt to provide secrets and config values.
#     Args:
#         secret_str (str, optional): Secret string provided by dlt.secrets.value. Defaults to dlt.secrets.value.
#         secret_dict (Dict[str, Any], optional): Secret dictionary provided by dlt.secrets.value. Defaults to dlt.secrets.value.
#         config_int (int, optional): Config integer provided by dlt.config.value. Defaults to dlt.config.value.
#     Returns:
#         DltResource: Returns a resource yielding the configured values.
#     """

#     # returns a resource yielding the configured values - it is just a test
#     return dlt.resource([secret_str, secret_dict, config_int], name="config_values")
