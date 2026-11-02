"""Worker classes for the tests of `worker_pool`, loaded in worker processes from this file"""

import os
import sys
from typing import Any, Dict, Iterator, Optional, Tuple

import pyarrow


class EchoWorker:
    """Streams a ticket back: a list of `(values, metadata)`, `values` None sends only metadata.
    A string ticket is raised as an error, "exit" ends the process."""

    def do_get(self, ticket: Any) -> Iterator[Tuple[Optional[Any], Dict[str, Any]]]:
        if ticket == "exit":
            os._exit(3)
        if isinstance(ticket, str):
            raise ValueError(ticket)
        for values, metadata in ticket:
            table = None if values is None else pyarrow.table({"n": values})
            yield table, {
                **metadata,
                "pid": os.getpid(),
                "modules": sorted(m for m in ("dlt", "sources") if m in sys.modules),
            }
