import dlt

from facebook_ads import (
    facebook_ads_source,
    facebook_insights_source,
    DEFAULT_ADCREATIVE_FIELDS,
    AdCreative,
    enrich_ad_objects,
)


def load_all_ads_objects() -> None:
    """Loads campaigns, ad sets, ads, ad creatives and leads"""
    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads",
        destination="duckdb",
        dataset_name="facebook_ads_data",
        dev_mode=True,
    )
    info = pipeline.run(facebook_ads_source())
    print(info)


def merge_ads_objects() -> None:
    """Shows how to convert the source into a merge one, where subsequent loads add or update records but not delete old ones"""
    pipeline = dlt.pipeline(
        pipeline_name="facebook_insights",
        destination="duckdb",
        dataset_name="facebook_insights_data",
        dev_mode=True,
    )
    fb_ads = facebook_ads_source()
    # enable root key propagation on a source that is not a merge one by default. this is not required if you always use merge but below we start
    # with replace
    fb_ads.root_key = True
    # load only disapproved ads
    fb_ads.ads.bind(states=("DISAPPROVED",))
    info = pipeline.run(fb_ads.with_resources("ads"), write_disposition="replace")
    # merge the paused ads. the disapproved ads stay there!
    fb_ads = facebook_ads_source()
    fb_ads.ads.bind(states=("PAUSED",))
    info = pipeline.run(fb_ads.with_resources("ads"), write_disposition="merge")
    print(info)
    # prove we have them all
    with pipeline.sql_client() as c:
        with c.execute_query("SELECT id, name, effective_status FROM ads") as q:
            print(q.df())


def load_ads_with_custom_fields() -> None:
    """Shows how to change the fields loaded for a particular object"""
    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads",
        destination="duckdb",
        dataset_name="facebook_ads_data",
        dev_mode=True,
    )
    fb_ads = facebook_ads_source()
    # only loads add ids, works the same for campaigns, leads etc.
    fb_ads.ads.bind(fields=("id",))
    info = pipeline.run(fb_ads.with_resources("ads"))
    print(info)


def load_only_disapproved_ads() -> None:
    """Shows how to load objects with a given statuses"""
    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads",
        destination="duckdb",
        dataset_name="facebook_ads_data",
        dev_mode=True,
    )
    fb_ads = facebook_ads_source()
    # we want only disapproved ads
    fb_ads.ads.bind(
        states=("DISAPPROVED",)
    )  # states=("DISAPPROVED", "PAUSED") for many states
    info = pipeline.run(fb_ads.with_resources("ads"))
    print(info)


def load_and_enrich_objects() -> None:
    """Show how to enrich objects by adding an enrichment transformation that adds fields to objects
    This (probably) goes around facebook limitations ie. it makes sense to get just ids for ad creatives and the rest via enrichments
    """
    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads",
        destination="duckdb",
        dataset_name="facebook_ads_data",
        dev_mode=True,
    )
    # also shows how to reduce chunk size: many small requests will be made
    fb_ads = facebook_ads_source(chunk_size=2)
    # request only id
    fb_ads.ad_creatives.bind(fields=("id",))
    # add a transformation to a ad_creatives resource
    fb_ads.ad_creatives.add_step(
        # pass AdCreative object type (you can import more object from `facebook_ads` ie Campaign, Ad etc.) and request the fields
        enrich_ad_objects(AdCreative, DEFAULT_ADCREATIVE_FIELDS)
    )
    info = pipeline.run(fb_ads.with_resources("ad_creatives"))
    print(info)


def load_insights() -> None:
    """Shows how to load daily and weekly insights with 7 days attribution window"""
    pipeline = dlt.pipeline(
        pipeline_name="facebook_insights",
        destination="duckdb",
        dataset_name="facebook_insights_data",
        dev_mode=True,
    )
    # just load 1 past day with attribution window of 7 days - that will re-acquire last 8 days + today
    i_daily = facebook_insights_source(initial_load_past_days=1)
    i_weekly = facebook_insights_source(initial_load_past_days=1, time_increment_days=7)
    info = pipeline.run([i_daily, i_weekly])
    print(info)


def load_insights_with_breakdowns() -> None:
    """Shows how to load insights with custom breakdowns and action breakdowns"""
    pipeline = dlt.pipeline(
        pipeline_name="facebook_insights_breakdowns",
        destination="duckdb",
        dataset_name="facebook_insights_data",
        dev_mode=True,
    )
    # Load insights with age and gender breakdowns
    i_with_breakdowns = facebook_insights_source(
        initial_load_past_days=7,
        breakdowns="ads_insights_age_and_gender",
        # Uncomment to add action breakdowns:
        # action_breakdowns=["action_type", "action_target_id"]
    )
    info = pipeline.run(i_with_breakdowns)
    print(info)


if __name__ == "__main__":
    # load_all_ads_objects()
    merge_ads_objects()
    # load_ads_with_custom_fields()
    # load_only_disapproved_ads()
    # load_and_enrich_objects()
    # load_insights()
    # load_insights_with_breakdowns()
