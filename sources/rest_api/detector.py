from dlt.sources.helpers.requests import Response

from .paginators import (
    HeaderLinkPaginator,
    JSONResponsePaginator,
    SinglePagePaginator,
)

RECORD_KEY_PATTERNS = {
    "data",
    "items",
    "results",
    "entries",
    "records",
    "rows",
    "entities",
    "payload",
}
NON_RECORD_KEY_PATTERNS = {
    "meta",
    "metadata",
    "pagination",
    "links",
    "extras",
    "headers",
}
NEXT_PAGE_KEY_PATTERNS = {"next", "nextpage", "nexturl"}
NEXT_PAGE_DICT_KEY_PATTERNS = {"href", "url"}


def find_all_lists(dict_, result=None, level=0):
    """Recursively looks for lists in dict_ and returns tuples
    in format (nesting level, dictionary key, list)
    """
    if level > 2:
        return []

    for key, value in dict_.items():
        if isinstance(value, list):
            result.append((level, key, value))
        elif isinstance(value, dict):
            find_all_lists(value, result, level + 1)

    return result


def find_records(response):
    # when a list was returned (or in rare case a simple type or null)
    if not isinstance(response, dict):
        return response
    lists = find_all_lists(response, result=[])
    if len(lists) == 0:
        # could not detect anything
        return response
    # we are ordered by nesting level, find the most suitable list
    try:
        return next(
            l[2]
            for l in lists
            if l[1] in RECORD_KEY_PATTERNS and l[1] not in NON_RECORD_KEY_PATTERNS
        )
    except StopIteration:
        # return the least nested element
        return lists[0][2]


def matches_any_pattern(key, patterns):
    normalized_key = key.lower()
    return any(pattern in normalized_key for pattern in patterns)


def find_next_page_key(dictionary, path=None):
    if not isinstance(dictionary, dict):
        return None

    if path is None:
        path = []

    for key, value in dictionary.items():
        if matches_any_pattern(key, NEXT_PAGE_KEY_PATTERNS):
            if isinstance(value, dict):
                for dict_key in value:
                    if matches_any_pattern(dict_key, NEXT_PAGE_DICT_KEY_PATTERNS):
                        return [*path, key, dict_key]
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
