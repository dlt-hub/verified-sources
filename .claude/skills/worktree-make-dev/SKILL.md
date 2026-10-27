---
name: worktree-make-dev
description: Set up a dev environment in an existing worktree (make dev + copy secrets)
argument-hint: <worktree-path>
---

# Set up dev environment in worktree

Prepare the worktree at `$ARGUMENTS` for development.

## Steps

### 1. Run setup script

```
bash tools/setup_worktree_env.sh <worktree-path>
```

The script runs `make dev` and copies `sources/.dlt/secrets.toml` without exposing credentials in the conversation. If it fails, report the error and stop.

### 2. Check for lock file changes

`make dev` upgrades `dlt` and may rewrite `uv.lock` and `tools/dependabot_lock/requirements.txt`. If the script output lists `CHANGED_BY_MAKE_DEV=` files, keep them out of the commit unless the change itself needs a lock update.

### 3. Report

Parse the script output for `SECRETS_SOURCE=` to determine what was used:

- **`repo_root`**: Copied from the main checkout — all sources and destinations configured there can be tested.
- **`example`**: Copied from `sources/.dlt/example.secrets.toml` — duckdb and local postgres (`tests/postgres/docker-compose.yml`) only; tests needing upstream credentials cannot run.
- **`none`**: No secrets found — warn the user

```
Dev environment ready: <worktree-path>
- make dev: done
- secrets.toml: <source>
```
