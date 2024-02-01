"""A source loading Datagog SLOs, monitors, test results and other nice things."""

import typing as t
from typing import Sequence, Iterable, Dict, Any
import dlt
from dlt.common.typing import TDataItem, TDataItems
from dlt.sources import DltResource
from dlt.sources.helpers import requests


from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v1.api.authentication_api import AuthenticationApi
from datadog_api_client.v1.api.service_level_objectives_api import (
    ServiceLevelObjectivesApi,
)
from datadog_api_client.v1.api.monitors_api import MonitorsApi
from datadog_api_client.v1.api.synthetics_api import SyntheticsApi


@dlt.source(name="datadog")
def datadog_source(
    api_key: str = dlt.secrets.value,
    application_key: str = dlt.secrets.value,
    site: str = dlt.config.value,
) -> Iterable[DltResource]:
    """Incrementally loads insight reports with defined granularity level, fields, breakdowns etc.

    By default, the reports are generated one by one for each day, starting with today - attribution_window_days_lag. On subsequent runs, only the reports
    from the last report date until today are loaded (incremental load). The reports from last 7 days (`attribution_window_days_lag`) are refreshed on each load to
    account for changes during attribution window.

    Mind that each report is a job and takes some time to execute.

    Args:
        api_key: str =  dlt.secrets.value,
        application_key: str = dlt.secrets.value,
        site: str = dlt.config.value

    Returns:
        DltResource: authentication
        DltResource: slos
        DltResource: monitors
        DltResource: synthetics
        DltResource: synthetic_tests

    """
    configuration = Configuration()
    configuration.api_key["apiKeyAuth"] = api_key
    configuration.api_key["appKeyAuth"] = application_key
    configuration.server_variables["site"] = site

    api_client = ApiClient(configuration)

    # return datadog_resource(api_key, application_key)

    @dlt.resource(name="authentication", write_disposition="append")
    def authentication() -> Iterable[TDataItem]:
        api_instance = AuthenticationApi(api_client)
        response = api_instance.validate()
        yield response.to_dict()

    yield authentication

    # this is going to reimport all the slos all the time with append
    @dlt.resource(name="slos", primary_key="id", write_disposition="append")
    def slos() -> Iterable[TDataItem]:
        api_instance = ServiceLevelObjectivesApi(api_client)
        response = api_instance.list_slos()

        yield response.to_dict()["data"]

    yield slos

    # transformer that retrieves a list of monitors
    @dlt.transformer(name="monitors", data_from=slos, write_disposition="replace")
    def monitors(slos: TDataItems) -> Iterable[TDataItem]:
        """Yields the monitors for a list of `slos`"""

        api_instance = MonitorsApi(api_client)

        for _slo in slos:
            for _monitor_id in _slo["monitor_ids"]:
                response = api_instance.get_monitor(monitor_id=_monitor_id)

                yield [response.to_dict()]

    yield monitors

    # transformer that retrieves a list of syntetics
    @dlt.transformer(name="synthetics", data_from=monitors, write_disposition="replace")
    def synthetics(monitors: TDataItems) -> Iterable[TDataItem]:
        """Yields the syntetics for a list of `monitors`"""

        api_instance = SyntheticsApi(api_client)

        # call and yield the function result normally, the @dlt.defer takes care of parallelism
        for _monitor in monitors:
            synthetic_public_id = _monitor["options"]["synthetics_check_id"]
            response = api_instance.get_test(public_id=synthetic_public_id)

            yield [response.to_dict()]

    yield synthetics

    # transformer that retrieves a list of syntetic tests result
    @dlt.transformer(
        name="synthetic_tests",
        data_from=synthetics,
        primary_key="result_id",
        write_disposition="merge",
    )
    def synthetic_tests(synthetics: TDataItems) -> Iterable[TDataItem]:
        """Yields the tests results for a list of `syntetics`"""

        api_instance = SyntheticsApi(api_client)

        def _get_test_results(_synthetic: TDataItem) -> Iterable[TDataItem]:
            if _synthetic["type"] == "api":
                api_results = api_instance.get_api_test_latest_results(
                    public_id=_synthetic["public_id"]
                ).to_dict()
            else:
                api_results = api_instance.get_browser_test_latest_results(
                    public_id=_synthetic["public_id"]
                ).to_dict()

            results = [
                {
                    "public_id": _synthetic["public_id"],
                    "last_timestamp_fetched": api_results["last_timestamp_fetched"],
                    **result,
                }
                for result in api_results["results"]
            ]

            return results

        # call and yield the function result normally, the @dlt.defer takes care of parallelism
        for _synthetic in synthetics:
            yield _get_test_results(_synthetic)

    yield synthetic_tests
