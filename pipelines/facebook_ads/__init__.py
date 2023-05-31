"""Loads campaigns, ads sets, ads, leads and insight data from Facebook Marketing API"""

import functools
import itertools
from typing import Any, Iterator, Sequence

from facebook_business import FacebookAdsApi
from facebook_business.api import FacebookResponse

import dlt
from dlt.common import pendulum, logger
from dlt.common.typing import TDataItems, TDataItem, DictStrAny
from dlt.common.time import parse_iso_like_datetime
from dlt.extract.typing import ItemTransformFunctionWithMeta
from dlt.extract.source import DltResource

from .fb import (
    execute_job,
    get_ads_account,
    AbstractObject,
    AbstractCrudObject,
    Ad,
    Campaign,
    AdSet,
    AdCreative,
    Lead,
)
from .fb import debug_access_token, get_long_lived_token
from .settings import (
    DEFAULT_AD_FIELDS,
    DEFAULT_ADCREATIVE_FIELDS,
    DEFAULT_ADSET_FIELDS,
    DEFAULT_CAMPAIGN_FIELDS,
    DEFAULT_LEAD_FIELDS,
    TFbMethod,
    TInsightsBreakdownOptions,
)
from .settings import (
    FACEBOOK_INSIGHTS_RETENTION_PERIOD,
    ALL_ACTION_BREAKDOWNS,
    ALL_ACTION_ATTRIBUTION_WINDOWS,
    DEFAULT_INSIGHT_FIELDS,
    INSIGHTS_PRIMARY_KEY,
    INSIGHTS_BREAKDOWNS_OPTIONS,
    INVALID_INSIGHTS_FIELDS,
    TInsightsLevels,
)


@dlt.source(name="facebook_ads")
def facebook_ads_source(
    account_id: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
    chunk_size: int = 50,
    request_timeout: float = 300.0,
) -> Sequence[DltResource]:
    """Returns a list of resources to load campaigns, ad sets, ads, creatives and ad leads data from Facebook Marketing API.

    All the resources have `replace` write disposition by default and define primary keys. Resources are parametrized and allow the user
    to change the set of fields that will be loaded from the API and the object statuses that will be loaded. See the demonstration script for details.

    You can convert the source into merge resource to keep the deleted objects. Currently Marketing API does not return deleted objects. See the demo script.

    We also provide a transformation `enrich_ad_objects` that you can add to any of the resources to get additional data per object via `object.get_api`

    Args:
        account_id (str, optional): Account id associated with add manager. See README.md
        access_token (str, optional): Access token associated with the Business Facebook App. See README.md
        chunk_size (int, optional): A size of the page and batch request. You may need to decrease it if you request a lot of fields. Defaults to 50.
        request_timeout (float, optional): Connection timeout. Defaults to 300.0.

    Returns:
        Sequence[DltResource]: campaigns, ads, ad_sets, ad_creatives, leads

    """
    account = get_ads_account(account_id, access_token, request_timeout)

    def _get_data_chunked(
        method: TFbMethod, fields: Sequence[str], states: Sequence[str]
    ) -> Iterator[TDataItems]:
        # add pagination and chunk into lists
        params: DictStrAny = {"limit": chunk_size}
        if states:
            params.update({"effective_status": states})
        it: map[DictStrAny] = map(lambda c: c.export_all_data(), method(fields=fields, params=params))  # type: ignore
        while True:
            chunk = list(itertools.islice(it, chunk_size))
            if not chunk:
                break
            yield chunk

    @dlt.resource(primary_key="id", write_disposition="replace")
    def campaigns(
        fields: Sequence[str] = DEFAULT_CAMPAIGN_FIELDS, states: Sequence[str] = None
    ) -> Iterator[TDataItems]:
        yield _get_data_chunked(account.get_campaigns, fields, states)

    @dlt.resource(primary_key="id", write_disposition="replace")
    def ads(
        fields: Sequence[str] = DEFAULT_AD_FIELDS, states: Sequence[str] = None
    ) -> Iterator[TDataItems]:
        yield _get_data_chunked(account.get_ads, fields, states)

    @dlt.resource(primary_key="id", write_disposition="replace")
    def ad_sets(
        fields: Sequence[str] = DEFAULT_ADSET_FIELDS, states: Sequence[str] = None
    ) -> Iterator[TDataItems]:
        yield _get_data_chunked(account.get_ad_sets, fields, states)

    @dlt.transformer(primary_key="id", write_disposition="replace", selected=True)
    def leads(
        items: TDataItems,
        fields: Sequence[str] = DEFAULT_LEAD_FIELDS,
        states: Sequence[str] = None,
    ) -> Iterator[TDataItems]:
        for item in items:
            ad = Ad(item["id"])
            yield _get_data_chunked(ad.get_leads, fields, states)

    @dlt.resource(primary_key="id", write_disposition="replace")
    def ad_creatives(
        fields: Sequence[str] = DEFAULT_ADCREATIVE_FIELDS, states: Sequence[str] = None
    ) -> Iterator[TDataItems]:
        yield _get_data_chunked(account.get_ad_creatives, fields, states)

    return campaigns, ads, ad_sets, ad_creatives, ads | leads


def enrich_ad_objects(
    fb_obj_type: AbstractObject, fields: Sequence[str]
) -> ItemTransformFunctionWithMeta[TDataItems]:
    """Returns a transformation that will enrich any of the resources returned by `` with additional fields

    In example below we add "thumbnail_url" to all objects loaded by `ad_creatives` resource:
    >>> fb_ads = facebook_ads_source()
    >>> fb_ads.ad_creatives.add_step(enrich_ad_objects(AdCreative, ["thumbnail_url"]))

    Internally, the method uses batch API to get data efficiently. Refer to demo script for full examples

    Args:
        fb_obj_type (AbstractObject): A Facebook Business object type (Ad, Campaign, AdSet, AdCreative, Lead). Import those types from this module
        fields (Sequence[str]): A list/tuple of fields to add to each object.

    Returns:
        ItemTransformFunctionWithMeta[TDataItems]: A transformation function to be added to a resource with `add_step` method
    """

    def _wrap(items: TDataItems, meta: Any = None) -> TDataItems:
        api_batch = FacebookAdsApi.get_default_api().new_batch()

        def update_item(resp: FacebookResponse, item: TDataItem) -> None:
            item.update(resp.json())

        def fail(resp: FacebookResponse) -> None:
            raise resp.error()

        for item in items:
            o: AbstractCrudObject = fb_obj_type(item["id"])
            o.api_get(
                fields=fields,
                batch=api_batch,
                success=functools.partial(update_item, item=item),
                failure=fail,
            )
        api_batch.execute()
        return items

    return _wrap


@dlt.source(name="facebook_ads")
def facebook_insights_source(
    account_id: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
    initial_load_past_days: int = 30,
    fields: Sequence[str] = DEFAULT_INSIGHT_FIELDS,
    attribution_window_days_lag: int = 7,
    time_increment_days: int = 1,
    breakdowns: TInsightsBreakdownOptions = "ads_insights_age_and_gender",
    action_breakdowns: Sequence[str] = ALL_ACTION_BREAKDOWNS,
    level: TInsightsLevels = "ad",
    action_attribution_windows: Sequence[str] = ALL_ACTION_ATTRIBUTION_WINDOWS,
    batch_size: int = 50,
    request_timeout: int = 300,
) -> DltResource:
    """Incrementally loads insight reports with defined granularity level, fields, breakdowns etc.

    By default, the reports are generated one by one for each day, starting with today - attribution_window_days_lag. On subsequent runs, only the reports
    from the last report date until today are loaded (incremental load). The reports from last 7 days (`attribution_window_days_lag`) are refreshed on each load to
    account for changes during attribution window.

    Mind that each report is a job and takes some time to execute.

    Args:
        account_id: str = dlt.config.value,
        access_token: str = dlt.secrets.value,
        initial_load_past_days (int, optional): How many past days (starting from today) to intially load. Defaults to 30.
        fields (Sequence[str], optional): A list of fields to include in each reports. Note that `breakdowns` option adds fields automatically. Defaults to DEFAULT_INSIGHT_FIELDS.
        attribution_window_days_lag (int, optional): Attribution window in days. The reports in attribution window are refreshed on each run.. Defaults to 7.
        time_increment_days (int, optional): The report aggregation window in days. use 7 for weekly aggregation. Defaults to 1.
        breakdowns (TInsightsBreakdownOptions, optional): A presents with common aggregations. See settings.py for details. Defaults to "ads_insights_age_and_gender".
        action_breakdowns (Sequence[str], optional): Action aggregation types. See settings.py for details. Defaults to ALL_ACTION_BREAKDOWNS.
        level (TInsightsLevels, optional): The granularity level. Defaults to "ad".
        action_attribution_windows (Sequence[str], optional): Attribution windows for actions. Defaults to ALL_ACTION_ATTRIBUTION_WINDOWS.
        batch_size (int, optional): Page size when reading data from particular report. Defaults to 50.
        request_timeout (int, optional): Connection timeout. Defaults to 300.

    Returns:
        DltResource: facebook_insights

    """
    account = get_ads_account(account_id, access_token, request_timeout)

    # we load with a defined lag
    initial_load_start_date = pendulum.today().subtract(days=initial_load_past_days)
    initial_load_start_date_str = initial_load_start_date.isoformat()

    @dlt.resource(primary_key=INSIGHTS_PRIMARY_KEY, write_disposition="merge")
    def facebook_insights(
        date_start: dlt.sources.incremental[str] = dlt.sources.incremental(
            "date_start", initial_value=initial_load_start_date_str
        )
    ) -> Iterator[TDataItems]:
        start_date = parse_iso_like_datetime(date_start.start_value).subtract(
            days=attribution_window_days_lag
        )

        # facebook forgets insights so trim the lag and warn
        min_start_date = pendulum.today().subtract(
            months=FACEBOOK_INSIGHTS_RETENTION_PERIOD
        )
        if start_date < min_start_date:
            logger.warning(
                "%s: Start date is earlier than %s months ago, using %s instead. "
                "For more information, see https://www.facebook.com/business/help/1695754927158071?id=354406972049255",
                "facebook_insights",
                FACEBOOK_INSIGHTS_RETENTION_PERIOD,
                min_start_date,
            )
            start_date = min_start_date
            date_start.start_value = min_start_date

        # lag the incremental start date by attribution window lag
        date_start.start_value = start_date.isoformat()

        end_date = pendulum.now()

        def _proc_report_item(item: AbstractObject) -> DictStrAny:
            d: DictStrAny = item.export_all_data()
            for pki in INSIGHTS_PRIMARY_KEY:
                if pki not in d:
                    d[pki] = "no_" + pki

            return d

        def _get_insights_for_day(day: pendulum.DateTime) -> Sequence[DictStrAny]:
            query = {
                "level": level,
                "action_breakdowns": list(action_breakdowns),
                "breakdowns": list(
                    INSIGHTS_BREAKDOWNS_OPTIONS[breakdowns]["breakdowns"]
                ),
                "limit": batch_size,
                "fields": list(
                    set(fields)
                    .union(INSIGHTS_BREAKDOWNS_OPTIONS[breakdowns]["fields"])
                    .difference(INVALID_INSIGHTS_FIELDS)
                ),
                "time_increment": time_increment_days,
                "action_attribution_windows": list(action_attribution_windows),
                "time_ranges": [
                    {
                        "since": day.to_date_string(),
                        "until": day.add(days=time_increment_days - 1).to_date_string(),
                    }
                ],
            }
            # print(query)
            job = execute_job(account.get_insights(params=query, is_async=True))
            return list(map(_proc_report_item, job.get_result()))

        while start_date <= end_date:
            yield _get_insights_for_day(start_date)
            start_date = start_date.add(days=time_increment_days)

    return facebook_insights
