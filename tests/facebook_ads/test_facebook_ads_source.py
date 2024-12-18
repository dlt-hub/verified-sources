import pytest

import dlt
import pendulum

from sources.facebook_ads import (
    facebook_ads_source,
    facebook_insights_source,
    get_start_date,
    enrich_ad_objects,
    Campaign,
    DEFAULT_CAMPAIGN_FIELDS,
)

from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.skip("We don't have a Facebook Ads test account.")
@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_all_ads_object(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads",
        destination=destination_name,
        dataset_name="facebook_ads_data",
        dev_mode=True,
    )
    info = pipeline.run(facebook_ads_source())
    assert_load_info(info)
    # assert tables created - we do not have any leads in test data
    expected_tables = ["campaigns", "ads", "ad_sets", "ad_creatives"]
    # all those tables in the schema
    assert (
        set(expected_tables)
        - set(t["name"] for t in pipeline.default_schema.data_tables())
        == set()
    )
    # get counts
    table_counts = load_table_counts(pipeline, *expected_tables)
    # all tables loaded
    assert set(table_counts.keys()) == set(expected_tables)
    assert all(c > 0 for c in table_counts.values())


@pytest.mark.skip("We don't have a Facebook Ads test account.")
def test_set_ads_fields() -> None:
    s = facebook_ads_source()
    # get only ids for ads
    s.ads.bind(fields=("id"))
    ads = list(s.with_resources("ads"))
    # all ads have only "id" field
    for ad in ads:
        assert len(ad) == 1
        assert "id" in ad


@pytest.mark.skip("We don't have a Facebook Ads test account.")
def test_select_only_ads_with_state() -> None:
    s = facebook_ads_source()
    all_ads = list(s.with_resources("ads"))
    s = facebook_ads_source()
    # get only ids for ads
    s.ads.bind(states=("DISAPPROVED",))
    disapproved_ads = list(s.with_resources("ads"))
    assert len(all_ads) > len(disapproved_ads)


@pytest.mark.skip("We don't have a Facebook Ads test account.")
def test_enrich_objects_multiple_chunks() -> None:
    s = facebook_ads_source()
    s.campaigns.bind(fields=("id",))
    campaigns = list(s.with_resources("campaigns"))
    assert len(campaigns) == 4

    # will load in 2 batches
    s = facebook_ads_source(chunk_size=2)
    s.campaigns.bind(fields=("id",))
    assert campaigns == list(s.with_resources("campaigns"))

    s = facebook_ads_source(chunk_size=2)
    s.campaigns.bind(fields=("id",))
    s.campaigns.add_step(enrich_ad_objects(Campaign, DEFAULT_CAMPAIGN_FIELDS))
    full_campaigns = list(s.with_resources("campaigns"))
    assert len(full_campaigns) == len(campaigns)
    # enriched props are there
    assert "name" in full_campaigns[0]


@pytest.mark.skip("We don't have a Facebook Ads test account.")
def test_load_insights() -> None:
    # just load 1 past day with attribution window of 7 days - that will re-acquire last 8 days + today
    i_daily = facebook_insights_source(initial_load_past_days=1)
    assert len(list(i_daily)) == 0


@pytest.mark.skip("We don't have a Facebook Ads test account.")
def test_load_insights_weekly() -> None:
    i_weekly = facebook_insights_source(initial_load_past_days=1, time_increment_days=7)
    assert len(list(i_weekly)) == 0


def test_get_start_date() -> None:
    # Test with an ISO datetime string
    input_value = "2023-08-09T12:30:00"
    result = get_start_date(
        incremental_start_date=dlt.sources.incremental("start_date", input_value),
        attribution_window_days_lag=7,
    )
    assert isinstance(result, pendulum.DateTime)
    assert result.year == 2023
    assert result.month == 8
    assert result.day == 2
    assert result.hour == 12
    assert result.minute == 30
    assert result.second == 0
    assert result.timezone_name == "UTC"

    # Test with an ISO date string
    input_value = "2023-08-09"
    result = get_start_date(
        incremental_start_date=dlt.sources.incremental("start_date", input_value),
        attribution_window_days_lag=7,
    )
    assert isinstance(result, pendulum.DateTime)
    assert result.year == 2023
    assert result.month == 8
    assert result.day == 2
    assert result.hour == 00
    assert result.minute == 00
    assert result.second == 00
    assert result.timezone_name == "UTC"
