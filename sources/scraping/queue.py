import typing as t

from queue import Empty, Queue

from dlt.common import logger


# Please read more at https://mypy.readthedocs.io/en/stable/runtime_troubles.html#not-generic-runtime
T = t.TypeVar("T")

if t.TYPE_CHECKING:

    class _Queue(Queue[T]):
        pass

else:

    class _Queue(Queue, t.Generic[T]):
        pass


class QueueClosedError(Exception):
    pass


class ScrapingQueue(_Queue):
    def __init__(
        self,
        maxsize: int = 0,
        batch_size: int = 10,
        read_timeout: float = 1.0,
    ) -> None:
        super().__init__(maxsize)
        self.batch_size = batch_size
        self.read_timeout = read_timeout
        self._is_closed = False

    def get_batches(self) -> t.Iterator[t.Any]:
        """Batching helper can be wrapped as a dlt.resource

        Returns:
            Iterator[Any]: yields scraped items one by one
        """
        batch: t.List[T] = []
        while True:
            if len(batch) >= self.batch_size:
                yield batch

            try:
                if self.is_closed:
                    raise QueueClosedError("Queue is closed")

                item = self.get(timeout=self.read_timeout)
                batch.append(item)

                # Mark task as completed
                self.task_done()
            except Empty:
                logger.info(f"Queue has been empty for {self.read_timeout}s...")
                yield batch
            except QueueClosedError:
                logger.info("Queue is closed, stopping...")

                # Return the last batch before exiting
                yield batch
                break

    def close(self) -> None:
        self._is_closed = True

    @property
    def is_closed(self) -> bool:
        return self._is_closed
