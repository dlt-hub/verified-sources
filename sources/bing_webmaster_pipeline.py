import dlt
from bing_webmaster import source


def load_page_stats_example() -> None:
    """
    Constructs a pipeline that will load organic search traffic from Bing Webmaster
    for site_url and pages
    """

    # configure the pipeline: provide the destination and dataset name to which the data should go
    pipeline = dlt.pipeline(
        pipeline_name="bing_webmaster_page_stats",
        destination="duckdb",
        dataset_name="bing_webmaster",
    )
    # create the data source by providing a list of site_urls.
    # Note that you have to first verify your own site urls. Thus, most likely,
    # you'll lack the permissions to request stats for the one provided in this example
    data = source(site_urls=["sipgate.de", "satellite.me"])
    # load the "page_stats" out of all the possible resources
    info = pipeline.run(data.with_resources("page_stats"))
    print(info)


def load_page_query_stats_example() -> None:
    """
    Constructs a pipeline that will load organic search traffic from Bing Webmaster
    for site_url, pages, and query
    """

    # configure the pipeline: provide the destination and dataset name to which the data should go
    pipeline = dlt.pipeline(
        pipeline_name="bing_webmaster_page_query_stats",
        destination="duckdb",
        dataset_name="bing_webmaster",
    )
    # create the data source by providing a list pairs of site_urls and pages.
    # Note that you have to first verify your own site urls. Thus, most likely,
    # you'll lack the permissions to request stats for the one provided in this example
    data = source(
        site_url_pages=[
            {"site_url": "sipgate.de", "page": "https://www.sipgate.de/preise"},
            {"site_url": "sipgate.de", "page": "https://www.sipgate.de/app"},
        ]
    )
    # load the "page_query_stats" out of all the possible resources
    info = pipeline.run(data.with_resources("page_query_stats"))
    print(info)


if __name__ == "__main__":
    load_page_stats_example()
    load_page_query_stats_example()
