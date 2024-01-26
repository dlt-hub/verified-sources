import threading
from typing import Generic, TypeVar, TYPE_CHECKING
from queue import Queue

# Please read more at https://mypy.readthedocs.io/en/stable/runtime_troubles.html#not-generic-runtime
T = TypeVar("T")

if TYPE_CHECKING:

    class _Queue(Queue[T]):
        pass
else:

    class _Queue(Generic[T], Queue):
        pass


class BaseQueue(_Queue[T]):
    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self._state_lock = threading.Lock()
        self._is_closed = False

    def close(self) -> None:
        with self._state_lock:
            if self._is_closed:
                return

            self._is_closed = True

    @property
    def is_closed(self) -> bool:
        with self._state_lock:
            return self._is_closed
