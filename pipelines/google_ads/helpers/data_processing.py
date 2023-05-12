from typing import Iterator
from dlt.common.typing import DictStrAny, TDataItem


def process_dimension(batch) -> Iterator[TDataItem]:
    """
    Processes a batch result (page of results per dimension) accordingly
    :param batch:
    :return:
    """

    for row in batch.results:
        customer_data = row.customer
        attributes = dir(customer_data)
        json_customer = {attribute: getattr(customer_data, attribute) for attribute in attributes if "__" not in attribute}
        processed_json_customer = {attribute: json_customer[attribute] if not hasattr(json_customer[attribute], "__dict__") else str(json_customer[attribute]) for attribute in json_customer.keys()}
        yield processed_json_customer


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
