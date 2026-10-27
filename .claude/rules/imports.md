---
paths:
  - "**/*.py"
---

# Import rules

## Import order

Groups separated by blank line:
1. **stdlib** (`import os`, `from typing import ...`)
2. **third-party** (`import dlt`, source-specific packages)
3. **local** (relative imports within source, or `from sources.X` in tests)
4. (tests only) **test utilities** (`from tests.utils import ...`)

Stdlib imports are ALWAYS at module level -- never inline in functions.

## Relative imports in source modules

Source folders under `sources/` are distributed as self-contained packages via `dlt init`. **Use relative imports** within a source module:

```python
# CORRECT -- inside sources/google_sheets/
from .helpers.data_processing import get_spreadsheet_id
from .helpers.api_calls import api_auth
from .helpers import api_calls
```

```python
# WRONG -- absolute imports break dlt init distribution
from sources.google_sheets.helpers import api_calls
```

## Source-specific dependencies

- Each source declares additional dependencies in its `requirements.txt` and in `pyproject.toml` dependency groups
- `dlt` is always available as a base dependency
- Third-party imports specific to a source (e.g., `scrapy`, `facebook_business`) are only available when that source's dependencies are installed
- When running or testing a specific source, activate its dependency group: `uv run --group <source_name>`

## Pipeline scripts

Pipeline scripts (`sources/<name>_pipeline.py`) sit outside the source package folder. They use **absolute imports** from the source package:

```python
from chess import chess_source
from scraping import run_pipeline
```

## Tests

Tests use absolute imports from `sources` and `tests`:

```python
from sources.scraping import run_pipeline
from tests.utils import ALL_DESTINATIONS, load_table_counts
```

## Logging

Use `from dlt.common import logger` for logging, not `import logging` directly.
