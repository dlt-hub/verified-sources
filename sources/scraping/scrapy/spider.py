from typing import Any, Optional, TypeVar
import scrapy  # type: ignore[import-untyped]

from ..types import BaseQueue

T = TypeVar("T")


class DltSpider(scrapy.Spider):
    """Default spider which will carry over a reference to queue in pipeline items"""

    def __init__(
        self,
        queue: BaseQueue[T],
        name: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, **kwargs)
        self.queue = queue
