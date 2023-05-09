"""Loads campaigns, ads sets, ads, leads and insight data from Facebook Marketing API"""

import functools
import itertools
from typing import Any, Iterator, Sequence

from facebook_business import FacebookAdsApi
from facebook_business.api import FacebookResponse

import dlt
# import requests
from dlt.common import pendulum, logger
from dlt.common.typing import TDataItems, TDataItem, DictStrAny
from dlt.common.time import parse_iso_like_datetime
from dlt.extract.typing import ItemTransformFunctionWithMeta
from dlt.extract.source import DltResource

from .fb import execute_job, get_ads_account, AbstractObject, AbstractCrudObject, Ad, Campaign, AdSet, AdCreative, Lead
from .fb import debug_access_token, get_long_lived_token
from .settings import DEFAULT_AD_FIELDS, DEFAULT_ADCREATIVE_FIELDS, DEFAULT_ADSET_FIELDS, DEFAULT_CAMPAIGN_FIELDS, DEFAULT_LEAD_FIELDS, TFbMethod, TInsightsBreakdownOptions
from .settings import (FACEBOOK_INSIGHTS_RETENTION_PERIOD, ALL_ACTION_BREAKDOWNS, ALL_ACTION_ATTRIBUTION_WINDOWS, DEFAULT_INSIGHT_FIELDS,
                       INSIGHTS_PRIMARY_KEY, INSIGHTS_BREAKDOWNS_OPTIONS, INVALID_INSIGHTS_FIELDS, TInsightsLevels)

"""
Authorization setup:
1. You must have Ads Manager active for your facebook account
2. Find your account id. It is against Account Overview dropdown or in the link ie. https://adsmanager.facebook.com/adsmanager/manage/accounts?act=10150974068878324
3. Create new facebook app. for that you need developers account
4. To get short lived access token use https://developers.facebook.com/tools/explorer/
5. Select app you just created
6. Select get user access token
7. Add permissions: `ads_read`, `leads_retrieval` (to retrieve the leads)
8. Generate token
9. Exchange the token with the link below
"""


@dlt.source(name="facebook_ads")
def facebook_ads_source(
    account_id: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
    chunk_size: int = 50,
    request_timeout: float = 300.0
) -> Sequence[DltResource]:

    account = get_ads_account(account_id, access_token, request_timeout)

    def _get_data_chunked(method: TFbMethod, fields: Sequence[str], states: Sequence[str]) -> Iterator[TDataItems]:
        # add pagination and chunk into lists
        params: DictStrAny = {"limit": chunk_size}
        if states:
            params.update({'effective_status': states})
        it: map[DictStrAny] = map(lambda c: c.export_all_data(), method(fields=fields, params=params))  # type: ignore
        while True:
            chunk = list(itertools.islice(it, chunk_size))
            if not chunk:
                break
            yield chunk

    @dlt.resource(primary_key="id", write_disposition="replace")
    def campaigns(fields: Sequence[str] = DEFAULT_CAMPAIGN_FIELDS, states: Sequence[str] = None) -> Iterator[TDataItems]:
        yield _get_data_chunked(account.get_campaigns, fields, states)

    @dlt.resource(primary_key="id", write_disposition="replace")
    def ads(fields: Sequence[str] = DEFAULT_AD_FIELDS, states: Sequence[str] = None) -> Iterator[TDataItems]:
        yield _get_data_chunked(account.get_ads, fields, states)

    @dlt.resource(primary_key="id", write_disposition="replace")
    def ad_sets(fields: Sequence[str] = DEFAULT_ADSET_FIELDS, states: Sequence[str] = None) -> Iterator[TDataItems]:
        yield _get_data_chunked(account.get_ad_sets, fields, states)

    @dlt.transformer(primary_key="id", write_disposition="replace", selected=True)
    def leads(items: TDataItems, fields: Sequence[str] = DEFAULT_LEAD_FIELDS, states: Sequence[str] = None) -> Iterator[TDataItems]:
        for item in items:
            ad = Ad(item["id"])
            yield _get_data_chunked(ad.get_leads, fields, states)

    @dlt.resource(primary_key="id", write_disposition="replace")
    def ad_creatives(fields: Sequence[str] = DEFAULT_ADCREATIVE_FIELDS, states: Sequence[str] = None) -> Iterator[TDataItems]:
        yield _get_data_chunked(account.get_ad_creatives, fields, states)

    return campaigns, ads, ad_sets, ad_creatives, ads | leads  # type: ignore


def enrich_ad_objects(fb_obj_type: AbstractObject, fields: Sequence[str]) -> ItemTransformFunctionWithMeta[TDataItems]:

    def _wrap(items: TDataItems, meta: Any = None) -> TDataItems:
        api_batch = FacebookAdsApi.get_default_api().new_batch()

        def update_item(resp: FacebookResponse, item: TDataItem) -> None:
            item.update(resp.json())

        def fail(resp: FacebookResponse) -> None:
            raise resp.error()

        for item in items:
            o: AbstractCrudObject = fb_obj_type(item["id"])
            o.api_get(fields=fields, batch=api_batch, success=functools.partial(update_item, item=item), failure=fail)
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
    request_timeout: int = 300
) -> DltResource:

    account = get_ads_account(account_id, access_token, request_timeout)

    # we load with a defined lag
    initial_load_start_date = pendulum.today().subtract(days=initial_load_past_days)
    initial_load_start_date_str = initial_load_start_date.isoformat()

    @dlt.resource(primary_key=INSIGHTS_PRIMARY_KEY, write_disposition="merge")
    def facebook_insights(
        date_start: dlt.sources.incremental[str] = dlt.sources.incremental("date_start", initial_value=initial_load_start_date_str)
    ) -> Iterator[TDataItems]:

        start_date = parse_iso_like_datetime(date_start.start_value).subtract(days=attribution_window_days_lag)

        # facebook forgets insights so trim the lag and warn
        min_start_date = pendulum.today().subtract(months=FACEBOOK_INSIGHTS_RETENTION_PERIOD)
        if start_date < min_start_date:
            logger.warning("%s: Start date is earlier than %s months ago, using %s instead. "
                            "For more information, see https://www.facebook.com/business/help/1695754927158071?id=354406972049255",
                        "facebook_insights",
                        FACEBOOK_INSIGHTS_RETENTION_PERIOD,
                        min_start_date)
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
                    'level': level,
                    'action_breakdowns': list(action_breakdowns),
                    'breakdowns': list(INSIGHTS_BREAKDOWNS_OPTIONS[breakdowns]["breakdowns"]),
                    'limit': batch_size,
                    'fields': list(set(fields).union(INSIGHTS_BREAKDOWNS_OPTIONS[breakdowns]["fields"]).difference(INVALID_INSIGHTS_FIELDS)),
                    'time_increment': time_increment_days,
                    'action_attribution_windows': list(action_attribution_windows),
                    'time_ranges': [{'since': day.to_date_string(), 'until': day.add(days=time_increment_days-1).to_date_string()}]
                }
            # print(query)
            job = execute_job(account.get_insights(params=query, is_async=True))
            return list(map(_proc_report_item, job.get_result()))

        while start_date <= end_date:
            yield _get_insights_for_day(start_date)
            start_date = start_date.add(days=time_increment_days)

    return facebook_insights  # type: ignore