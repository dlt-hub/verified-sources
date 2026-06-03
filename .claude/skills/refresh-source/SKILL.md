---
name: refresh-source
description: Audit a verified source against the current state of its upstream API/SDK, find breakages and outdated patterns, and propose (or implement) a fix plan with credential-free unit tests. Use when users report a source is outdated or broken, or for periodic source maintenance.
argument-hint: <source-name> -- [optional hints what to pay attention to]
---

# Refresh a verified source

Arguments: `$ARGUMENTS` — first token is the source folder name under `sources/`, anything after `--` is user hints (specific user reports, suspected areas).

Work through the phases in order. Do not skip phase 0 — wrong conclusions are usually formed by analyzing code that is no longer alive or pins that no longer reflect reality.

## Phase 0 — Ground truth

1. Read everything in `sources/<name>/`, `sources/<name>_pipeline.py`, `tests/<name>/`.
2. `git log --oneline -- sources/<name>` — look for past removals/refactors. **Hunt dead code**: files, fixtures, conftest helpers, docstrings and README claims orphaned by earlier PRs. Dead code should be deleted, never "fixed". Dead code may also be the only reason for a heavy dependency — deleting it can drop whole packages from `requirements.txt`.
3. Compare the THREE dependency declarations and flag mismatches:
   - `sources/<name>/requirements.txt` — what `dlt init` users actually install (often unbounded!)
   - `pyproject.toml` dependency group `<name>` — what CI tests
   - any version pinned in code (e.g. `api_version = "..."`)
   A CI group pinned years behind an unbounded requirements.txt means CI is testing a different client than users get — that is how breakage ships silently.
4. Check open/closed GitHub issues for the source (`gh search issues`) and any user hints from the arguments.

## Phase 1 — Research the upstream (web search, authoritative sources only)

Use WebSearch/WebFetch against official docs, SDK changelogs, and SDK migration guides (vendor docs site, vendor GitHub wiki/CHANGELOG). For each finding record the source URL.

- Current SDK major version and every breaking change between the version CI pins and today (migration guides are gold: object model changes, removed dict-inheritance, auth patterns, deprecated module-level clients).
- Current API version / versioning model; what the code pins vs. what is current; breaking changes in between (renamed/removed/restructured fields the source or its helpers depend on).
- Official request/response shapes the code touches: list envelopes, pagination cursors, filter parameter formats, retention limits (e.g. event retention windows). Capture documented example payloads — they become test fixtures in phase 4.

## Phase 2 — Reproduce empirically. Never claim a breakage you haven't run.

Use ephemeral environments to prove each suspected breakage and each proposed fix, at **both ends** of the allowed dependency range:

```sh
cd /tmp && uv run --with "<sdk>==<oldest-allowed>" python - <<'EOF' ... EOF
cd /tmp && uv run --with "<sdk>==<newest>" python - <<'EOF' ... EOF
```

Construct real SDK objects (e.g. `construct_from`-style factories) and exercise the exact code path that is suspected broken. Subtle version differences matter (e.g. a conversion helper being shallow in one major and recursive in another) — only execution reveals them.

## Phase 3 — Plan (present before implementing)

Tier the findings; keep tiers separable:

- **P0 — unbreak**: minimal fixes that work across the whole supported dependency range; align CI group with what fresh users install; fix requirements bounds (add upper bound to majors not yet verified). Must NOT change loaded table schemas.
- **P1 — modernize client**: new SDK client patterns, drop global state, configurable API version. Anything that changes response shapes **changes loaded schemas for existing users** — call this out explicitly as breaking and keep it out of P0.
- **P2 — features**: new endpoints, incremental improvements.

Prefer dlt built-ins over hand-rolled code (`dlt.common.time.ensure_pendulum_datetime`, `dlt.common` logger, rest_api helpers) — but verify the utility exists in the oldest dlt allowed by the source's requirements.txt before using it.

## Phase 4 — Tests (the biggest pain point)

Existing source tests are usually credential-gated integration tests that never run for contributors. Always add **credential-free unit tests**:

- New `tests/<name>/test_helpers.py` (module-based `def test_*() -> None`, parametrize with readable ids — see `.claude/rules/testing.md`).
- **Use proven shapes, not invented ones**: fixtures must mirror request/response shapes, pagination envelopes and cursor semantics found in official docs during phase 1 (cite which doc page a fixture mirrors when it isn't obvious).
- Mock at the transport/SDK boundary with **real SDK objects** (`construct_from` etc.), monkeypatching the API call — so the installed SDK's serialization actually runs instead of a MagicMock lying about it.
- Cover: pagination control flow (cursor passed from last item, stop on terminal page flag), parameter construction (filters, special-cased endpoints), and any date/type conversion helpers with one case per accepted input type.
- Run the unit tests against both ends of the SDK range (phase 2 envs) before declaring the range supported.

## Phase 5 — Verify and finish

1. `uv lock` if pyproject changed; ensure resolution works on all supported Python versions.
2. `pytest tests/<name>/test_helpers.py` — must pass without anything in `sources/.dlt`.
3. Integration tests `pytest tests/<name>` only if credentials exist; otherwise state plainly they were not run.
4. `make lint-code` and `make format-lint`.
5. Update stale docstrings/README touched by the changes. Report findings with file:line references and the doc URLs that back each claim.

## Phase 6 — Assess the public dlt docs

The user-facing docs live in the separate public `dlt-hub/dlt` repo at
`docs/website/docs/dlt-ecosystem/verified-sources/<name>.md` and are also served to LLMs via
https://dlthub.com/docs/llms.txt — one fix covers both. Fetch the file
(`gh api repos/dlt-hub/dlt/contents/docs/...`) and review it against the refreshed source:

- **Code drift**: constant names, default endpoint tuples, function signatures and links quoted in the docs must match `settings.py`/`__init__.py` exactly — docs commonly lag source-repo PRs (e.g. an endpoint moved between ENDPOINTS tuples).
- **Copy-pasteable examples are the highest-stakes content**: an example demonstrating a removed or unsafe pattern (e.g. incremental loading of an editable endpoint) silently produces wrong data for everyone who pastes it.
- **Document what the refresh established**: supported SDK version range, pinned upstream API version and its schema implications, accepted input formats, retention limits and other upstream constraints discovered in phase 1.
- Propose the changes as a diff against the fetched file; the actual PR goes to `dlt-hub/dlt`, not this repo.
