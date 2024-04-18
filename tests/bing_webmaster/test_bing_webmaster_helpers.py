import copy

from dlt.common import pendulum

from sources.bing_webmaster import helpers


def test_microsoft_date_parsing():
    parsed = helpers._parse_date({"Date": "/Date(1700179200000)/"})
    assert parsed == pendulum.Date(2023, 11, 17)


getPageStat_api_response = [
    {
        "__type": "QueryStats:#Microsoft.Bing.Webmaster.Api",
        "AvgClickPosition": 1,
        "AvgImpressionPosition": 1,
        "Clicks": 100,
        "Date": "/Date(1700179200000)/",
        "Impressions": 1000,
        "Query": "https://dlthub.com/why/",
    }
]


def test_parse_get_page_stats_response__replaces_query_with_page():
    parsed = list(
        helpers.parse_response(
            response=copy.deepcopy(getPageStat_api_response),
            site_url="dlthub.com",
        )
    )

    assert "page" in parsed[0]
    assert "Query" not in parsed[0]
    assert parsed[0].get("page") == getPageStat_api_response[0].get("Query")


def test_parse_get_page_stats_response__adds_site_url():
    parsed = list(
        helpers.parse_response(
            response=copy.deepcopy(getPageStat_api_response),
            site_url="dlthub.com",
        )
    )

    assert "site_url" not in getPageStat_api_response[0]
    assert parsed[0].get("site_url") == "dlthub.com"


def test_parse_get_page_stats_response():
    parsed = list(
        helpers.parse_response(
            response=getPageStat_api_response,
            site_url="dlthub.com",
        )
    )

    expected = [
        {
            "AvgClickPosition": 1,
            "AvgImpressionPosition": 1,
            "Clicks": 100,
            "Date": pendulum.Date(2023, 11, 17),
            "Impressions": 1000,
            "page": "https://dlthub.com/why/",
            "site_url": "dlthub.com",
        }
    ]

    assert parsed == expected


def test_parse_get_page_query_stats_response():
    api_response = [
        {
            "__type": "QueryStats:#Microsoft.Bing.Webmaster.Api",
            "AvgClickPosition": 1,
            "AvgImpressionPosition": 1,
            "Clicks": 100,
            "Date": "/Date(1700179200000)/",
            "Impressions": 1000,
            "Query": "dlt documentation",
        }
    ]
    parsed = list(
        helpers.parse_response(
            response=api_response,
            site_url="dlthub.com",
            page="https://dlthub.com/docs/intro",
        )
    )

    expected = [
        {
            "AvgClickPosition": 1,
            "AvgImpressionPosition": 1,
            "Clicks": 100,
            "Date": pendulum.Date(2023, 11, 17),
            "Impressions": 1000,
            "Query": "dlt documentation",
            "page": "https://dlthub.com/docs/intro",
            "site_url": "dlthub.com",
        }
    ]

    assert parsed[0].get("page") == "https://dlthub.com/docs/intro"
    assert parsed[0].get("site_url") == "dlthub.com"
    assert parsed == expected
