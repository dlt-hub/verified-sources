from typing import TYPE_CHECKING, Any, Callable, Generator, Generic, Optional, TypeVar
from queue import Queue
from scrapy.http import Response  # type: ignore


# Please read more at https://mypy.readthedocs.io/en/stable/runtime_troubles.html#not-generic-runtime
T = TypeVar("T")
if TYPE_CHECKING:

    class _Queue(Queue[T]):
        pass

else:

    class _Queue(Generic[T], Queue):
        pass


class BaseQueue(_Queue[T]):
    pass


OnNextPage = Callable[[Response], Optional[str]]
OnResult = Callable[[Response], Generator[Any, None, None]]
