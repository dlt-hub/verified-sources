"""A pipeline loading player profiles and games from chess.com api"""

from typing import Callable, Iterator, List, Sequence, Dict, Any

import dlt
from dlt.common import pendulum
from dlt.common.typing import TDataItem, StrAny
from dlt.extract.source import DltResource
from dlt.sources.helpers import requests

OFFICIAL_CHESS_API_URL = "https://api.chess.com/pub/"
UNOFFICIAL_CHESS_API_URL = "https://www.chess.com/callback/"


@dlt.source
def chess(players: List[str], start_month: str = None, end_month: str = None) -> Sequence[DltResource]:
    """A dlt source for the chess.com api. It groups several resources (in this case chess.com API endpoints) containing various types of data: ie user profiles
    or chess match results

    Args:
        chess_url (str): Url of the chess.com api
        players (list): A list of the player usernames for which to get the data
        start_month ("YYYY/MM", optional): Filters out all the matches happening before `start_month`
        end_month ("YYYY/MM", optional): Filters out all the matches happening after `end_month`

    Returns:
        A list of following resources that you can select from
            "players_profiles" - yields profiles of the `players`,
            "players_archives" - yields list of archives with games available to the `players`,
            "players_games" - yields games of `players` in specified time period,
            "players_online_status" - yields online status of players,
    """
    return (
        players_profiles(players),
        players_archives(players),
        players_games(players, start_month=start_month, end_month=end_month),
        players_online_status(players)
    )

def _get_url_with_retry(url: str) -> StrAny:
    r = requests.get(url)
    return r.json() # type: ignore


def _get_path_with_retry(path: str) -> StrAny:
    return _get_url_with_retry(f"{OFFICIAL_CHESS_API_URL}{path}")


@dlt.resource(write_disposition="replace")
def players_profiles(players: List[str]) -> Iterator[TDataItem]:
    """Yields player profiles for a list of player usernames"""

    # get archives in parallel by decorating the http request with defer
    @dlt.defer
    def _get_profile(username: str) -> TDataItem:
        return _get_path_with_retry(f"player/{username}")

    for username in players:
        yield _get_profile(username)


@dlt.resource(write_disposition="replace", selected=False)
def players_archives(players: List[str]) -> Iterator[List[TDataItem]]:
    """Yields url to game archives for specified players."""

    for username in players:
        data = _get_path_with_retry(f"player/{username}/games/archives")
        yield data.get("archives", [])


@dlt.resource(write_disposition="append")
def players_games(players: List[str], start_month: str = None, end_month: str = None) -> Iterator[Callable[[], List[TDataItem]]]:
    """Yields `players` games that happened between `start_month` and `end_month`. See the `chess` source documentation for details."""
    # do a simple validation to prevent common mistakes in month format
    if start_month and start_month[4] != "/":
        raise ValueError(start_month)
    if end_month and end_month[4] != "/":
        raise ValueError(end_month)

    # get a list of already checked archives, you will read more about the dlt.state on Day 3 of our workshop
    # from your point of view, the state is python dictionary that will have the same content the next time this function is called
    checked_archives = dlt.state().setdefault("archives", [])
    # get player archives, note that you can call the resource like any other function and just iterate it like a list
    archives = players_archives(players)

    # get archives in parallel by decorating the http request with defer
    @dlt.defer
    def _get_archive(url: str) -> List[TDataItem]:
        print(f"Getting archive from {url}")
        try:
            games = _get_url_with_retry(url).get("games", [])
            print(f"Got archive from {url}")
            return games  # type: ignore
        except requests.HTTPError as http_err:
            # sometimes archives are not available and the error seems to be permanent
            if http_err.response.status_code == 404:
                return []
            raise

    # enumerate the archives
    url: str = None
    for url in archives:
        # the `url` format is https://api.chess.com/pub/player/{username}/games/{YYYY}/{MM}
        if start_month and url[-7:] < start_month:
            continue
        if end_month and url[-7:] > end_month:
            continue
        # do not download archive again
        if url in checked_archives:
            continue
        else:
            checked_archives.append(url)
        # get the filtered archive
        yield _get_archive(url)


@dlt.resource(write_disposition="append")
def players_online_status(players: List[str]) -> Iterator[TDataItem]:
    """Returns current online status for a list of players"""
    # we'll use unofficial endpoint to get online status, the official seems to be removed
    for player in players:
        status = _get_url_with_retry("%suser/popup/%s" % (UNOFFICIAL_CHESS_API_URL, player))
        # return just relevant selection
        yield {
            "username": player,
            "onlineStatus": status["onlineStatus"],
            "lastLoginDate": status["lastLoginDate"],
            "check_time": pendulum.now()  # dlt can deal with native python dates
        }


@dlt.source
def chess_dlt_config_example(secret_str: str = dlt.secrets.value, secret_dict: Dict[str, Any] = dlt.secrets.value, config_int: int = dlt.config.value) -> DltResource:
    """This is an example of a source that uses dlt to provide secrets and config values. Please check `pipelines/.dlt/config.toml` and `pipelines./.dlt/example.secrets.toml`
    to see how those values are specified.
    """
    print(secret_str)
    print(secret_dict)
    print(config_int)

    # returns a resource yielding the configured values - it is just a test
    return dlt.resource([secret_str, secret_dict, config_int], name="config_values")
