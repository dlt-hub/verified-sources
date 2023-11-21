"""Fetches Personio Employees, Absences, Attendances."""

from typing import Iterable, Optional

import dlt
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.typing import TAnyDateTime, TDataItem
from dlt.extract.source import DltResource

from .helpers import PersonioApi
from .settings import DEFAULT_ITEMS_PER_PAGE, FIRST_DAY_OF_MILLENNIUM


@dlt.source(name="personio")
def personio_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    items_per_page: int = DEFAULT_ITEMS_PER_PAGE,
) -> Iterable[DltResource]:
    """
    The source for the Personio pipeline. Available resources are employees, absences, and attendances.

    Args:
        client_id: The client ID of your app.
        client_secret: The client secret of your app.
        items_per_page: The max number of items to fetch per page. Defaults to 200.
    Returns:
        Iterable: A list of DltResource objects representing the data resources.
    """

    client = PersonioApi(client_id, client_secret)

    @dlt.resource(primary_key="id", write_disposition="merge")
    def employees(
        updated_at: dlt.sources.incremental[
            pendulum.DateTime
        ] = dlt.sources.incremental(
            "last_modified_at", initial_value=None, allow_external_schedulers=True
        ),
        items_per_page: int = items_per_page,
    ) -> Iterable[TDataItem]:
        """
        The resource for employees, supports incremental loading and pagination.

        Args:
            updated_at: The saved state of the last 'last_modified_at' value.
            items_per_page: The max number of items to fetch per page. Defaults to 200.

        Returns:
            Iterable: A generator of employees.
        """
        if updated_at.last_value:
            last_value = updated_at.last_value.format("YYYY-MM-DDTHH:mm:ss")
        else:
            last_value = None

        params = {"updated_since": last_value}

        yield from client.get_pages(
            "company/employees", params=params, page_size=items_per_page
        )

    @dlt.resource(primary_key="id", write_disposition="merge")
    def absences(items_per_page: int = items_per_page) -> Iterable[TDataItem]:
        """
        The resource for absences, supports incremental loading and pagination.

        Args:
            items_per_page: The max number of items to fetch per page. Defaults to 200.

        Returns:
            Iterable: A generator of absences.
        """

        yield from client.get_pages(
            "company/time-off-types", page_size=items_per_page, convert_response=False
        )

    @dlt.resource(primary_key="id", write_disposition="merge")
    def attendances(
        start_date: TAnyDateTime = FIRST_DAY_OF_MILLENNIUM,
        end_date: Optional[TAnyDateTime] = None,
        updated_at: dlt.sources.incremental[
            pendulum.DateTime
        ] = dlt.sources.incremental(
            "last_modified_at", initial_value=None, allow_external_schedulers=True
        ),
        items_per_page: int = items_per_page,
    ) -> Iterable[TDataItem]:
        """
        The resource for attendances, supports incremental loading and pagination.

        Args:
            start_date: The start date to fetch attendances from.
            end_date: The end date to fetch attendances from. Defaults to now.
            updated_at: The saved state of the last 'last_modified_at' value.
            items_per_page: The max number of items to fetch per page. Defaults to 200.

        Returns:
            Iterable: A generator of attendances.
        """

        end_date = end_date or pendulum.now()
        if updated_at.last_value:
            updated_iso = updated_at.last_value.format("YYYY-MM-DDTHH:mm:ss")
        else:
            updated_iso = None

        params = {
            "start_date": ensure_pendulum_datetime(start_date).to_date_string(),
            "end_date": ensure_pendulum_datetime(end_date).to_date_string(),
            "updated_from": updated_iso,
        }

        yield from client.get_pages(
            "company/attendances", params=params, page_size=items_per_page
        )

    return (employees, absences, attendances)
