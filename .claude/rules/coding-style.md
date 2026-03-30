---
paths:
  - "**/*.py"
---

# Coding style

## Formatting
- Line length: 100 chars (black + isort, profile "black")
- Use `make format-lint` to format and lint before committing

## Code and patterns reuse
- Each verified source is self-contained in its own folder under `sources/`
- When modifying a source, look at existing code in that source folder for patterns and style
- When using a util function or any other function from the code base, look at existing usage if in doubt how it works
- Common test utilities are in `tests/utils.py`

## Comments
- Inline comments start with lowercase (`# resolve the schema`)
- NEVER add comments that separate code blocks (`# -- section name --`)
- DO NOT state the obvious, do not document your thought process

## Types and data structures
- Always use full generic parametrization -- `Dict[str, Any]` not `dict`
- `TypedDict` for structured dicts, `NamedTuple` for lightweight immutable data
- Code must be typed -- mypy is run on all sources
- Prefer `functools.partial` over `lambda` when currying
