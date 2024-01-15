from typing import Any, Dict, List, Optional

import scrapy  # type: ignore

from .types import BaseQueue


class DLTSpiderBase(scrapy.Spider):
    """Base spider which requires additional
    dependencies and subsequent spiders should
    implement their own parse logic with sending
    the extracted data over the queue.
    """

    def __init__(
        self,
        queue: BaseQueue,
        name: Optional[str] = None,
        settings: Optional[Dict[str, Any]] = None,
        start_urls: Optional[List[str]] = None,
        **kwargs: Any,
    ):
        super().__init__(name, **kwargs)
        self._queue = queue
        self.custom_settings = settings or {}
        self.start_urls = start_urls or []

    def send_data(self, data: Any) -> None:
        self._queue.put(data)

    def done(self) -> None:
        self.send_data({"done": True})
