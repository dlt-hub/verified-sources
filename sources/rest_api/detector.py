from dlt.sources.helpers.requests import Response

from .paginators import (
    HeaderLinkPaginator,
    JSONResponsePaginator,
    SinglePagePaginator,
)

RECORD_KEY_PATTERNS = {"data", "items", "results", "entries"}
NEXT_PAGE_KEY_PATTERNS = {"next", "nextpage", "nexturl"}


def find_records_key(dictionary, path=None):
    if not isinstance(dictionary, dict):
        return None

    if path is None:
        path = []

    for key, value in dictionary.items():
        # Direct match
        if key in RECORD_KEY_PATTERNS:
            return [*path, key]

        if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
            return [*path, key]

        if isinstance(value, dict):
            result = find_records_key(value, [*path, key])
            if result:
                return result

    return None


def find_next_page_key(dictionary, path=None):
    if not isinstance(dictionary, dict):
        return None

    if path is None:
        path = []

    for key, value in dictionary.items():
        normalized_key = key.lower()
        if any(pattern in normalized_key for pattern in NEXT_PAGE_KEY_PATTERNS):
            return [*path, key]

        if isinstance(value, dict):
            result = find_next_page_key(value, [*path, key])
            if result:
                return result

    return None


def header_links_detector(response: Response):
    links_next_key = "next"

    if response.links.get(links_next_key):
        return HeaderLinkPaginator()
    return None


def json_links_detector(response: Response):
    dictionary = response.json()
    next_key = find_next_page_key(dictionary)

    if not next_key:
        return None

    return JSONResponsePaginator(next_key=next_key)


def single_page_detector(response: Response):
    value = response.json()
    if isinstance(value, list):
        return SinglePagePaginator()

    return None


def create_paginator(response: Response):
    rules = [
        header_links_detector,
        json_links_detector,
        single_page_detector,
    ]
    for rule in rules:
        paginator = rule(response)
        if paginator:
            return paginator

    return None
