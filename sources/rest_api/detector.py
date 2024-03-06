from abc import abstractmethod

from dlt.sources.helpers.requests import Response

RECORD_KEY_PATTERNS = {"data", "items", "results", "entries"}
NEXT_PAGE_KEY_PATTERNS = {"next", "nextpage", "nexturl"}


class PaginatorDetectorMixin:
    @classmethod
    @abstractmethod
    def detect(cls, response: Response):
        pass


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
