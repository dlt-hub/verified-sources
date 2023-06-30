import dlt
import os
from typing import Dict, List, Sequence, Iterable
from pathlib import Path
from dlt.extract.source import DltResource, DltSource, TDataItem


@dlt.resource(write_disposition="replace")
def local_folder(data_dir=dlt.secrets.value, extensions: Sequence = (".txt",)):
    files = (p.resolve() for p in Path(data_dir).glob("**/*") if p.suffix in set(extensions))
    for file in files:
        yield {"file_path": str(file)}
