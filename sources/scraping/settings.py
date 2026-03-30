from .types import AnyDict

SOURCE_BATCH_SIZE: int = 10
SOURCE_SCRAPY_QUEUE_SIZE: int = 3000
SOURCE_SCRAPY_QUEUE_RESULT_TIMEOUT: int = 5
SOURCE_SCRAPY_SETTINGS: AnyDict = {
    "TELNETCONSOLE_ENABLED": False,
    # How many sub pages to scrape
    # https://docs.scrapy.org/en/latest/topics/settings.html#depth-limit
    "DEPTH_LIMIT": 0,
    "SPIDER_MIDDLEWARES": {
        "scrapy.spidermiddlewares.depth.DepthMiddleware": 200,
        "scrapy.spidermiddlewares.httperror.HttpErrorMiddleware": 300,
    },
    "HTTPERROR_ALLOW_ALL": True,
    "FAKEUSERAGENT_PROVIDERS": [
        # this is the first provider we'll try
        "scrapy_fake_useragent.providers.FakeUserAgentProvider",
        # if FakeUserAgentProvider fails, we'll use faker to generate a user-agent string for us
        "scrapy_fake_useragent.providers.FakerProvider",
        # fall back to USER_AGENT value
        "scrapy_fake_useragent.providers.FixedUserAgentProvider",
    ],
    "USER_AGENT": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
}
