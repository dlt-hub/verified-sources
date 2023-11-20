"""
This source provides data extraction from an example source as a starting point for new pipelines.
Available resources: [berries, pokemon]
"""

import typing as t
from typing import Sequence, Iterable, Dict, Any
import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource
from dlt.sources.helpers import requests
from .settings import BERRY_URL, POKEMON_URL


@dlt.resource(write_disposition="replace")
def berries() -> Iterable[TDataItem]:
    """
    Returns a list of berries.
    Yields:
        dict: The berries data.
    """
    yield requests.get(BERRY_URL).json()["results"]


@dlt.resource(write_disposition="replace")
def pokemon() -> Iterable[TDataItem]:
    """
    Returns a list of pokemon.
    Yields:
        dict: The pokemon data.
    """
    yield requests.get(POKEMON_URL).json()["results"]


@dlt.source
def source() -> Sequence[DltResource]:
    """
    The source function that returns all availble resources.
    Returns:
        Sequence[DltResource]: A sequence of DltResource objects containing the fetched data.
    """
    return [berries, pokemon]
