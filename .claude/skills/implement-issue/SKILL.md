---
name: implement-issue
description: Triage, plan, and implement a fix or feature for a GitHub issue end-to-end
argument-hint: <issue-number> [-- <hints or context from maintainer>]
---

# Implement Issue

Parse `$ARGUMENTS` to extract:
- Everything before the first `--` (or all of it if no `--`) is the **issue number**. Referred to as `ISSUE` below.
- Everything after the first `--` is **maintainer hints** — domain knowledge, suspected root cause, pointers to relevant code, or constraints. If absent, there are no hints.

## Steps

### 1. Fetch the issue

```
gh issue view ISSUE --json title,body,comments,labels,state,assignees,author,createdAt,milestone
```

Produce a concise summary: title, reporter, affected source, error message, reproduction steps, expected behavior.

Check whether open PRs already address the issue:

```
gh pr list --state open --search "ISSUE"
```

If one exists, report it and ask the human whether to review it with `/review-pr` instead of implementing from scratch.

### 2. Determine category and gather hints

#### Category

Derive the branch category from the issue labels:

| Label contains | Category |
|---|---|
| `bug` | `fix` |
| `enhancement`, `source new feature` | `feat` |

If no matching label exists, ask the human for the category.

Issues labeled `source request` or `verified source` ask for a new source. New sources are accepted only when they cannot be easily implemented with `rest_api`, `sql_database` or `filesystem` (see `README.md`) — stop and discuss scope with the human before going further. Build an accepted source with [refresh-source](../refresh-source/SKILL.md), which also handles new sources.

#### Hints

Collect implementation hints from all available sources, in priority order:

1. **Maintainer hints** from `$ARGUMENTS` (after `--`)
2. **Issue comments** — look for maintainer/contributor replies with technical guidance, root cause analysis, or implementation suggestions
3. **Issue body** — the reporter may include debugging details, stack traces, or configuration

If the combined hints from these sources provide sufficient direction (suspected root cause, relevant code areas, constraints), proceed directly to step 3 without asking the human.

If hints are insufficient — the issue is vague, the root cause is unclear, or there are multiple possible approaches — ask the human for guidance.

### 3. Research the big picture

Before jumping to a fix, verify assumptions. This step prevents wasted effort on the wrong solution.

#### 3a. Understand the source

Use Explore agents to thoroughly investigate the relevant code paths in `sources/<name>/`, the demo script `sources/<name>_pipeline.py` and `tests/<name>/`. Trace the flow from the source/resource functions to the point of failure. Read the code — don't guess.

#### 3b. Validate the reporter's setup

Check whether the reporter is using the source correctly:

- Is their configuration valid for the source?
- Did they install what `sources/<name>/requirements.txt` declares, and does their dlt version satisfy it?
- Does the documentation describe the expected behavior differently? The user docs live in the `dlt-hub/dlt` repo under `docs/website/docs/dlt-ecosystem/verified-sources/`; fetch them with `gh api repos/dlt-hub/dlt/contents/<path>?ref=devel`.

#### 3c. Check the upstream system and dlt

- If the issue involves the upstream API, SDK or database, consult its official documentation and changelogs via WebFetch/WebSearch. If the source is broadly outdated against its upstream API, suggest [refresh-source](../refresh-source/SKILL.md) instead.
- If the issue involves dlt behavior (state, incremental loading, `add_limit`, schema hints), read the dlt code installed in `.venv` rather than assuming how it works.

#### 3d. Determine: bug or user error?

Based on research, conclude one of:

- **Bug**: The source has a defect. Proceed to step 4.
- **User error**: The reporter's configuration is wrong. Draft a response explaining the correct approach. Ask the human to confirm before posting.
- **Enhancement**: The source works as designed but could be improved. Discuss scope with the human before proceeding. Enhancements that change how the source reads or loads data follow [refresh-source](../refresh-source/SKILL.md).

Report findings to the human before moving on.

### 4. Set up workspace

Before planning, create the working environment so plan-mode exploration runs against a clean branch.

#### 4a. Determine branch name

Use the category from step 2 and the issue number:
```
{category}/{issue-number}-{short-description}
```

Example: `fix/703-pg-replication-batch-progress`

If `ISSUE` was already a branch name rather than a number (no ticket exists), use it as given.

#### 4b. Create worktree and dev environment

Invoke the `/worktree-from-issue` skill with the branch name:
```
/worktree-from-issue {branch-name}
```

This creates the worktree, checks out a new branch from `origin/master`, and runs `/worktree-make-dev`.
Use it also when there is no issue number.

Note the worktree path — referred to as `WORKTREE` below — and which credentials are available from the `/worktree-make-dev` output.

Verify that the cwd is now inside the worktree:

```
pwd
```

If `pwd` does not show `WORKTREE`, run `cd WORKTREE` and verify again. Stop with an error if the cwd cannot be set. Once confirmed, subsequent Bash calls will run inside the worktree automatically.

### 5. Draft implementation plan

Enter plan mode and design a plan that **will pass the `/review-pr` skill**. The review-pr skill checks:

1. **Code quality**: Style conforming to `.claude/rules/` and `CONTRIBUTING.md`, no over-engineering, proper use of existing patterns and dlt utilities.
2. **Distribution**: The source folder stays self-contained with relative imports, so `dlt init` keeps working.
3. **Test coverage**: Required scenarios identified, tests in the right location, no duplication, proper parametrization.
4. **Breaking changes**: Changes to loaded schema, source/resource signatures or stored state are flagged, see [Data shape and upgrade compatibility](../refresh-source/SKILL.md#data-shape-and-upgrade-compatibility).
5. **Dependencies**: New packages declared in both `sources/<name>/requirements.txt` and the `pyproject.toml` group, `uv.lock` updated.
6. **Documentation**: The source README stays super short; user-facing changes need a docs update in `dlt-hub/dlt`.

The plan must include:

- **Working environment**: Worktree path, branch name, and `cd` instruction (see `.claude/rules/worktree.md`).
- **Root cause**: Precise code location and explanation.
- **Fix**: Minimal change following existing patterns in the source.
- **Test**: A test that **fails without the fix and passes with it**. Prefer a credential-free test that runs the resource through dlt with the upstream mocked at the API/transport boundary, so it runs for contributors and on fork CI. Place it where similar functionality is already tested in `tests/<name>/`.
- **Test scope**: Which tests need upstream credentials and whether they are available in the worktree. Use `ALL_DESTINATIONS='["duckdb"]'` unless the fix is destination-specific; local postgres is available via `tests/postgres/docker-compose.yml`.
- **Verification commands**: Exact selective pytest commands to run (specific test functions, not entire modules).
- **Docs**: Whether the `dlt-hub/dlt` docs page or the demo script needs an update.
- **Files changed**: List with expected line counts.

Present the plan via ExitPlanMode for human approval.

### 6. Implement the fix

Apply the changes from the approved plan. Follow these rules:

- **Read before editing**: Always read the target file first. Never edit blind.
- **Minimal changes**: Only change what the plan specifies. Don't refactor surrounding code, add comments to unchanged code, or "improve" things not in scope.
- **Follow local patterns**: Match the style of surrounding code exactly — imports, naming, spacing, comment style.
- **Imports**: Relative imports inside `sources/<name>/`, `from sources.<name> import ...` in tests (see `.claude/rules/imports.md`).
- **Type hints**: All new function signatures need full type hints with generic parametrization (`Dict[str, Any]` not `dict`).
- **Test conventions**: `def test_*() -> None:`, pytest fixtures, `@pytest.mark.parametrize` for variants, `dev_mode` pipelines (see `.claude/rules/testing.md`).
- **Dependencies**: Add packages with `uv add --group <name> <package>` and to `sources/<name>/requirements.txt`.

### 7. Verify

Run verification in this order. Stop and fix issues before proceeding to the next step.

#### 7a. Run the new test selectively

Run only the new/changed test functions using the commands from the plan. Be selective — target specific test functions, not entire modules.

#### 7b. Regression proof (for external system interactions)

When the fix involves an external system (upstream API, SDK, database) where you don't have full visibility into the behavior — a "black box" — verify experimentally that the test actually catches the bug:

- Temporarily revert the fix
- Run the new test on a configuration where the bug manifests
- Confirm it **fails** with an error matching the issue report
- Restore the fix

This is valuable when you cannot fully predict how the external system handles the input (e.g., how Postgres decodes a replication message, how an API responds to edge-case payloads). It proves the test is meaningful and not a no-op.

Skip this step when the code path is fully internal and you can reason about correctness from the code alone.

#### 7c. Format and lint

```
make format && make lint
```

`make lint` runs `make lint-code` (package structure, mypy, flake8, black) and `make lint-dlt-init` (`check-requirements.py` and the `dlt init` tests). All must pass cleanly. Fix any issues before reporting.

#### 7d. Docs

If the change affects user-facing behavior, draft the update to the `dlt-hub/dlt` docs page as a diff against the fetched file. Do not open a PR in `dlt-hub/dlt` without asking the human.

### 8. Report

Summarize results:

```
## Fix for #ISSUE: <title>

**Branch**: {branch-name}
**Worktree**: WORKTREE

### Changes
- `<file>`: <description> (<lines> lines)
- ...

### Verification
- <test-function> on <config>: passed
- <test-function> on <config> without fix: FAILED (confirms bug) [if applicable]
- tests not run (no credentials): <list> [if applicable]
- make format: pass
- make lint: pass

### Docs
- dlt-hub/dlt update: <not needed | drafted below>

Ready to commit.
```

Wait for the human to confirm before committing.
