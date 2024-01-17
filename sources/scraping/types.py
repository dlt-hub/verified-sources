from typing import TYPE_CHECKING, Any, Callable, Generator, Optional, TypeVar
from queue import Queue
from scrapy.http import Response  # type: ignore


# Please read more at https://mypy.readthedocs.io/en/stable/runtime_troubles.html#not-generic-runtime
if TYPE_CHECKING:
    T = TypeVar("T")
    BaseQueue = Queue[T]
else:
    BaseQueue = Queue


OnNextPage = Callable[[Response], Optional[str]]
OnResult = Callable[[Response], Generator[Any, None, None]]
