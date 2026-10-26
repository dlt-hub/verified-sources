#!/usr/bin/env bash
# Set up a dev environment in a git worktree:
#   1. Run make dev
#   2. Copy sources/.dlt/secrets.toml (repo root or example fallback)
#
# Usage: tools/setup_worktree_env.sh <worktree-path>

set -euo pipefail

WORKTREE="${1:?Usage: $0 <worktree-path>}"
WORKTREE="$(cd "$WORKTREE" && pwd)"

# find the main worktree (repo root) -- the first entry in `git worktree list`
# sed drains the stream; `head` would SIGPIPE git on long listings under `pipefail`
REPO_ROOT="$(git -C "$WORKTREE" worktree list --porcelain | sed -n '1s/^worktree //p')"
if [ "$REPO_ROOT" = "$WORKTREE" ]; then
    # we ARE the main worktree, no separate repo root to copy from
    REPO_ROOT=""
fi

echo "==> Worktree: $WORKTREE"
echo "==> Repo root: $REPO_ROOT"

# uv picks the newest interpreter for a fresh venv; match the main checkout instead
if [ -z "${UV_PYTHON:-}" ] && [ -n "$REPO_ROOT" ] && [ -x "$REPO_ROOT/.venv/bin/python" ]; then
    UV_PYTHON="$("$REPO_ROOT/.venv/bin/python" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
    export UV_PYTHON
    echo "==> Python: $UV_PYTHON (from repo root .venv)"
fi

# 1. make dev
echo "==> Running make dev..."
(cd "$WORKTREE" && make dev)

# make dev upgrades dlt, which may rewrite tracked lock files
CHANGED="$(git -C "$WORKTREE" status --porcelain -- uv.lock tools/dependabot_lock | sed 's/^...//' | tr '\n' ' ')"
if [ -n "$CHANGED" ]; then
    echo "CHANGED_BY_MAKE_DEV=$CHANGED"
fi

# 2. copy secrets -- tests and demo pipelines read them from sources/.dlt
SECRETS_DST="$WORKTREE/sources/.dlt/secrets.toml"
SECRETS_SRC_REPO="${REPO_ROOT:+$REPO_ROOT/sources/.dlt/secrets.toml}"
SECRETS_SRC_EXAMPLE="$WORKTREE/sources/.dlt/example.secrets.toml"

if [ -n "$SECRETS_SRC_REPO" ] && [ -f "$SECRETS_SRC_REPO" ]; then
    cp "$SECRETS_SRC_REPO" "$SECRETS_DST"
    echo "==> secrets.toml: copied from repo root"
    echo "SECRETS_SOURCE=repo_root"
elif [ -f "$SECRETS_SRC_EXAMPLE" ]; then
    cp "$SECRETS_SRC_EXAMPLE" "$SECRETS_DST"
    echo "==> secrets.toml: copied from example.secrets.toml (local destinations only)"
    echo "SECRETS_SOURCE=example"
else
    echo "==> WARNING: no secrets source found"
    echo "SECRETS_SOURCE=none"
fi

echo "==> Done"
