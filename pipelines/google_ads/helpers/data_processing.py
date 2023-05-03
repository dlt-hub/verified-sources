from typing import Iterator
from dlt.common.typing import DictStrAny, TDataItem


def process_dimension(batch) -> Iterator[TDataItem]:
    """
    Processes a batch result (page of results per dimension) accordingly
    :param batch:
    :return:
    """

    for row in batch.results:
        yield row


def process_report(batch) -> Iterator[TDataItem]:
    """
    Processes a batch result (page of results per report) accordingly
    :param batch:
    :return:
    """

    for row in batch.results:
        yield row


def process_query(batch) -> Iterator[TDataItem]:
    """
    Processes a batch result (page of results per query) accordingly
    :param batch:
    :return:
    """

    for row in batch.results:
        yield row
