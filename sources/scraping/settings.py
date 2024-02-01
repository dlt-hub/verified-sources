from .types import AnyDict

SOURCE_BATCH_SIZE: int = 10
SOURCE_SCRAPY_QUEUE_SIZE: int = 3000
SOURCE_SCRAPY_QUEUE_RESULT_TIMEOUT: int = 5
SOURCE_SCRAPY_SETTINGS: AnyDict = {
    "LOG_LEVEL": "INFO",
    # If not set then will keep logging warning in the console
    # https://docs.scrapy.org/en/latest/topics/request-response.html#request-fingerprinter-implementation
    "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
    "TELNETCONSOLE_ENABLED": False,
}
