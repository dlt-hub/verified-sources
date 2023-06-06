import pytest

import dlt

from sources.facebook_ads import (
    facebook_ads_source,
    facebook_insights_source,
    enrich_ad_objects,
    Campaign,
    DEFAULT_CAMPAIGN_FIELDS,
)

from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_load_all_ads_object(destination_name: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads",
        destination=destination_name,
        dataset_name="facebook_ads_data",
        full_refresh=True,
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


def test_set_ads_fields() -> None:
    s = facebook_ads_source()
    # get only ids for ads
    s.ads.bind(fields=("id"))
    ads = list(s.with_resources("ads"))
    # all ads have only "id" field
    for ad in ads:
        assert len(ad) == 1
        assert "id" in ad


def test_select_only_ads_with_state() -> None:
    s = facebook_ads_source()
    all_ads = list(s.with_resources("ads"))
    s = facebook_ads_source()
    # get only ids for ads
    s.ads.bind(states=("DISAPPROVED",))
    disapproved_ads = list(s.with_resources("ads"))
    assert len(all_ads) > len(disapproved_ads)


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


def test_load_insights() -> None:
    # just load 1 past day with attribution window of 7 days - that will re-acquire last 8 days + today
    i_daily = facebook_insights_source(initial_load_past_days=1)
    assert len(list(i_daily)) == 0


def test_load_insights_weekly() -> None:
    i_weekly = facebook_insights_source(initial_load_past_days=1, time_increment_days=7)
    assert len(list(i_weekly)) == 0
