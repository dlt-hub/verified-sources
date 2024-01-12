from typing import TYPE_CHECKING, TypeVar
from queue import Queue

# Please read more at https://mypy.readthedocs.io/en/stable/runtime_troubles.html#not-generic-runtime
if TYPE_CHECKING:
    T = TypeVar("T")
    BaseQueue = Queue[str]
else:
    BaseQueue = Queue
