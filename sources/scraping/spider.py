import logging
from typing import Any, Dict, List, Optional, TypeVar

import scrapy  # type: ignore
from scrapy.http import Response  # type: ignore

from .types import BaseQueue, OnNextPage, OnResult

logger = logging.getLogger(__file__)

T = TypeVar("T")


class DLTSpiderBase(scrapy.Spider):
    """Base spider which requires additional
    dependencies and subsequent spiders should
    implement their own parse logic with sending
    the extracted data over the queue.
    """

    def __init__(
        self,
        queue: BaseQueue[T],
        name: Optional[str] = None,
        settings: Optional[Dict[str, Any]] = None,
        start_urls: Optional[List[str]] = None,
        include_headers: bool = False,
        **kwargs: Any,
    ):
        super().__init__(name, **kwargs)
        self._queue = queue
        self.custom_settings = settings or {}
        self.start_urls = start_urls or []
        self.include_headers = include_headers

    def send_data(self, data: Any) -> None:
        self._queue.put(data)

    def done(self) -> None:
        self.send_data({"done": True})


class DLTSpider(DLTSpiderBase):
    """
    This is generic Dlt spider which
    delegates parsing and figuring out the
    next page to user defined callbacks.
    """

    def __init__(  # type: ignore[no-untyped-def]
        self,
        *args,
        on_next_page: OnNextPage,
        on_result: OnResult,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.on_result = on_result
        self.on_next_page = on_next_page

    def parse(self, response: Response, **kwargs: Any) -> Any:
        # We need to call callback to parse results
        logger.debug("Calling on_result", extra={"current_url": response.url})
        base_headers = {
            "status": response.status,
            **(
                dict(response.headers.to_unicode_dict()) if self.include_headers else {}
            ),
        }
        for parsed_result in self.on_result(response):
            self.send_data(
                {
                    "headers": base_headers,
                    **parsed_result,
                }
            )

        # If next page is available
        # Then we create next request
        # Else we stop spider because no pages left
        next_page = self.on_next_page(response)
        if next_page is not None:
            next_page = response.urljoin(next_page)
            logger.debug(
                "Got next page link",
                extra={"current_url": response.url, "next_url": next_page},
            )
            yield scrapy.Request(next_page, callback=self.parse)
        else:
            # Important: once we are done we need to send signal
            # to pipeline so it can finalize and shutdown
            logger.info("No pages left, done.")
            self.done()
