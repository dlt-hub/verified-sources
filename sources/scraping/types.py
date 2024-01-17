from typing import TYPE_CHECKING, Any, Callable, Generator, Optional, TypeVar
from queue import Queue
from scrapy.http import Response  # type: ignore


# Please read more at https://mypy.readthedocs.io/en/stable/runtime_troubles.html#not-generic-runtime
# Mypy will also complain down the line about generic T being not subscriptable please read
# https://github.com/python/mypy/issues/5264#issuecomment-399407428
if TYPE_CHECKING:
    T = TypeVar("T")
    BaseQueue = Queue[T]
    class _Queue:
        def __getitem__(*args):
            return Queue

    BaseQueue = _Queue()
else:
    BaseQueue = Queue


OnNextPage = Callable[[Response], Optional[str]]
OnResult = Callable[[Response], Generator[Any, None, None]]
