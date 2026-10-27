---
name: review-pr
description: Analyze a GitHub pull request including diff, comments, related issues, and local code context
argument-hint: <pr-url-or-number> [-- <optional instructions>]
---

# Review Pull Request

Parse `$ARGUMENTS` to extract the PR identifier and optional reviewer instructions:
- Everything before the first `--` (or all of it, if there is no `--`) is the **PR identifier** (URL or number). Referred to as `PR_ID` below.
- Everything after the first `--` is **reviewer instructions** — additional focus areas or context from the caller. If absent, there are no special instructions.

Analyze the pull request: PR_ID

The review is read-only. Most PRs here come from forks: do not run the PR's code, tests or pipeline scripts unless the human asks for it.

## Steps

Run the following steps. Gather data in parallel where possible.

### 1. Set up worktree

Resolve the PR number:

```
gh pr view PR_ID --json number
```

Use the number to form the worktree name `review-pr-<number>`.

Invoke the `/create-worktree review-pr-<number> --pr <number>` skill to create an isolated worktree for this PR. Note the worktree path from the skill output — referred to as `WORKTREE` below.

Verify that the cwd is now inside the worktree:

```
pwd
```

If `pwd` does not show `WORKTREE`, run `cd WORKTREE` and verify again. Stop with an error if the cwd cannot be set. Once confirmed, subsequent Bash calls will run inside the worktree automatically.

### 2. Fetch PR metadata and diff

Use `gh` to collect all PR information in parallel:

- `gh pr view PR_ID --json title,body,author,state,baseRefName,headRefName,isCrossRepository,number,url,comments,reviews,labels,milestone,closingIssuesReferences,files`
- `gh pr diff PR_ID`

Identify the **affected sources**: folder names under `sources/` touched by the diff. Most of the remaining steps are scoped to them.

### 3. Check dependency changes

Run this step if the diff touches `pyproject.toml`, `uv.lock` or any `sources/<name>/requirements.txt`, or if it adds an import of a third-party package to a source.

Each source declares its dependencies twice, and both must agree:
- `sources/<name>/requirements.txt` — what `dlt init` users install. Changes here affect every user of the source.
- `[dependency-groups].<name>` in `pyproject.toml` — what CI installs. A group is only installed by CI when it is listed in `[tool.uv].default-groups`.

Check:
- Every third-party package imported by the source is declared in **both** files.
- `dlt` in `requirements.txt` has a version range and no extras. If the change uses dlt features, the minimum `dlt` version must already provide them.
- `uv.lock` is up to date with `pyproject.toml`: run `uv lock --check` in `WORKTREE`.
- Changes to `[project.dependencies]` or to shared groups (`dltpure`, `pytest`, `dev`, `default-groups`) affect CI for all sources.

Report `requirements.txt` changes and shared group changes as WARNING. Report source group-only changes for awareness.

If none of the trigger files changed and no new imports were added, skip this step.

### 4. Check CI status

Use `gh pr checks PR_ID` to retrieve the CI check results. The relevant checks:

- `Runs mypy, flake and bandit` — `make lint`: package structure, mypy, flake8, black, `check-requirements.py` and the `dlt init` tests (`.github/workflows/lint.yml`).
- `tests dlt init compatibility` — `dlt init` against the affected sources (`.github/workflows/init.yml`).
- `test destinations postgres and duckdb` (PRs from this repo) or `FORKS - test on local postgres and duckdb` (PRs from forks) — tests of the affected sources only.
- `test destinations bigquery and redshift` — slow destination tests.

Report the status of each check (pass/fail/pending/skipped).

- If a check failed, fetch its logs with `gh run view <run-id> --log-failed` and identify the specific errors.
- For fork PRs, the test workflow only runs after a maintainer adds the `ci from fork` label. When its jobs show as skipped, report that **no tests ran on CI** — never count a skipped check as passing. Recommend adding the label; do not add it yourself.
- Source tests that need upstream credentials only pass on CI when the secrets for that source exist. Note when the PR's tests depend on credentials CI may not have.

Include CI status in the final review verdict. A failing lint or `dlt init` check is a required fix.

### 5. Analyze related issues

Collect the issues from `closingIssuesReferences` and any issue references in the PR body (e.g. `fixes #1234`, `closes #1234`, `#1234`). For each referenced issue, fetch it with:

- `gh issue view <number> --json title,body,author,state,comments,labels`

Understand the original problem being solved. Also check whether other open PRs address the same issue: `gh pr list --state open --search "<issue-number>"`.

### 6. Analyze the code

Using absolute paths under `WORKTREE`:

- Read the changed files in full to understand context beyond the diff.
- Look at callers/callees of modified functions.
- Check if similar patterns exist elsewhere that should also be updated.
- Review the coding style and code patterns against `.claude/rules/` (coding style, imports, testing) and `CONTRIBUTING.md`.
- Check that the change survives `dlt init` distribution (`docs/DISTRIBUTION.md`): the source folder stays self-contained, uses relative imports, and imports nothing from other sources or from `tests`. The demo script `sources/<name>_pipeline.py` uses absolute imports from the source package.
- If **reviewer instructions** were provided, pay special attention to the areas or concerns they describe during this analysis.
- Mention only crucial findings to the user in the initial overview. Avoid code snippets except if critical bugs were found.

### 6a. Pay attention to special cases

- **New source** — read [new-source-checklist.md](new-source-checklist.md) when the diff adds a new folder under `sources/`. It is intentionally not loaded otherwise.
- **Loaded schema** — changes to resource or table names, column names or types, primary or merge keys, write dispositions or nesting change the data that existing users already loaded. Flag each one as breaking.
- **State and incremental cursors** — changes to resource state keys, incremental cursor fields or initial values must stay compatible with the state that existing pipelines already stored. Check what happens on the first run after an upgrade.
- For both, follow [Data shape and upgrade compatibility](../refresh-source/SKILL.md#data-shape-and-upgrade-compatibility): what to compare, how to observe an upgrade and how to classify each change.
- **Version and changelog** — every PR that changes a source must bump `sources/<name>/version.py` (added with `1.0.0` if missing) and, unless it is a patch version, add a changelog entry to its README, see [Source version and README](../refresh-source/SKILL.md#source-version-and-readme). A breaking change needs a `major` bump and its upgrade path in the changelog. A missing bump or entry blocks the merge.

### 7. Review test coverage

#### 7a. Define the testing scope

Based on the PR changes, identify what needs to be tested:

- What are the distinct behaviors introduced or modified?
- What are the edge cases and error paths?
- What are the integration boundaries (e.g., does the change affect how the source interacts with dlt extract, state or the upstream API)?

Produce a checklist of required test scenarios before evaluating existing tests.

#### 7b. Evaluate existing tests against the checklist

- Compare the PR's tests against the required scenarios checklist. Flag any gaps.
- Flag any tests that verify things outside the PR's scope or test obvious/trivial behavior that adds no value.
- Most source tests need upstream credentials and do not run for contributors or on fork CI. Prefer credential-free unit tests that mock at the API/transport boundary with realistic payloads.
- Prefer tests that run the resource through dlt (`pipeline.extract` or `pipeline.run` on duckdb) over tests that patch dlt internals such as `dlt.current.resource_state` by hand. Hand-patched dlt behavior hides how dlt actually handles state, limits and generator closing.

#### 7c. Verify tests are in the right modules

- Tests live in `tests/<source_name>/` and import the source as `from sources.<name> import ...`.
- Search the source's test folder for where similar functionality is already tested (grep for related function names, class names, or feature keywords).
- Check the layout in [Test modules](../../rules/testing.md#test-modules): unit test modules follow source modules, end-to-end tests only in `test_<name>_source.py`, non-local destinations parametrized with `destination_name`.
- Flag tests placed in the wrong location.

#### 7d. Check for duplication and over-testing

- Look for tests that cover nearly identical cases and could be merged or parametrized with `@pytest.mark.parametrize`.
- Look for repeated setup/assertion logic across tests that should be extracted into a fixture or helper (`tests/<source_name>/conftest.py` or `utils.py`, not the shared `tests/utils.py`).
- If multiple tests differ only in a single variable (e.g., `True`/`False`, different type values), they must be parametrized into one test function.
- Flag any copy-paste test code. Minimizing test code maintenance burden is a priority.
- Flag tests that are not parallel safe: pipelines without `dev_mode`, fixed names for slots, schemas or accounts, missing cleanup.

#### 7e. Bug fixes should have regression tests
If the PR fixes a bug:
- Is there a test that would have caught the bug before the fix?
- Does the test verify the specific scenario from the bug report?

### 8. Check documentation coverage

Documentation is split between two repos:

- **Source README** (`sources/<name>/README.md`) — the maintenance document of the source: changelog, extension points, seeding, tests, operations, see [Source version and README](../refresh-source/SKILL.md#source-version-and-readme). Check that the PR updates the sections it affects. Setup guides, configuration reference and usage examples belong in the dlt docs.
- **User docs** — live in the `dlt-hub/dlt` repo under `docs/website/docs/dlt-ecosystem/verified-sources/`, as `<doc>.md` or a `<doc>/` folder. The doc name does not always match the source folder (e.g. `stripe_analytics` → `stripe.md`, `asana_dlt` → `asana.md`, `kinesis` → `amazon_kinesis.md`). List and fetch them with `gh api repos/dlt-hub/dlt/contents/docs/website/docs/dlt-ecosystem/verified-sources?ref=devel`.

If the PR changes user-facing behavior (source or resource arguments, defaults, loaded schema, setup steps, required permissions, dependencies):

- Fetch the doc page and check whether it describes the changed behavior.
- If the docs need an update, flag that a companion PR to `dlt-hub/dlt` (based on `devel`) is required and outline the change.
- Check that the demo script `sources/<name>_pipeline.py` still matches the changed API.
- If the topic is not documented at all, this is not a blocker — don't require docs for previously undocumented internals.

### 9. Produce the review

Write a structured analysis covering:

- **Summary**: What the PR does and why, referencing the linked issue.
- **Changes**: Breakdown of changes should be concise, avoid code samples.
- **Dependency Changes**: Findings from step 3. `requirements.txt` and shared group changes must appear as WARNING blocks. Omit this section if no dependency changes were found.
- **CI**: Status of lint, `dlt init` and test checks. If any failed, list the specific errors. State plainly when tests did not run.
- **Observations & Potential Issues**: Bugs, edge cases, missing error handling, performance concerns, memory issues, security issues.
- **Breaking Changes**: Changes to source/resource signatures, loaded schema or stored state that affect existing users (step 6a), each with what fails after an upgrade and the upgrade path.
- **Test Coverage**: Required scenarios checklist with pass/fail status, misplaced tests, duplication issues, parametrization opportunities.
- **Documentation**: version bump and changelog entry, README sections the change affects, whether a `dlt-hub/dlt` docs update is needed, demo script.
- **Reviewer Instructions**: If reviewer instructions were provided, include a dedicated section addressing each point raised. If no instructions were provided, omit this section.
- **Verdict**: Overall assessment.
