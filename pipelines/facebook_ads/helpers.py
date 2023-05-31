import functools
import itertools
from typing import Any, Iterator, Sequence

import dlt
import pendulum
from dlt.common import logger
from dlt.common.time import parse_iso_like_datetime
from dlt.common.typing import DictStrAny, TDataItem, TDataItems
from dlt.extract.typing import ItemTransformFunctionWithMeta
from facebook_business import FacebookAdsApi
from facebook_business.api import FacebookResponse

from .fb import AbstractCrudObject, AbstractObject
from .settings import (
    FACEBOOK_INSIGHTS_RETENTION_PERIOD,
    INSIGHTS_PRIMARY_KEY,
    TFbMethod,
)


def get_start_date(
    incremental_start_date: dlt.sources.incremental[str],
    attribution_window_days_lag: int = 7,
) -> pendulum.DateTime:
    """
    Get the start date for incremental loading of Facebook Insights data.
    """
    start_date: pendulum.DateTime = parse_iso_like_datetime(
        incremental_start_date.start_value
    ).subtract(days=attribution_window_days_lag)

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
        incremental_start_date.start_value = min_start_date

    # lag the incremental start date by attribution window lag
    incremental_start_date.start_value = start_date.isoformat()
    return start_date


def process_report_item(item: AbstractObject) -> DictStrAny:
    d: DictStrAny = item.export_all_data()
    for pki in INSIGHTS_PRIMARY_KEY:
        if pki not in d:
            d[pki] = "no_" + pki

    return d


def get_data_chunked(
    method: TFbMethod, fields: Sequence[str], states: Sequence[str], chunk_size: int
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
