#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

chmod +x \
  "$REPO_DIR/.claude/hooks/openspec_context.py" \
  "$REPO_DIR/.claude/hooks/openspec_router.py" \
  "$REPO_DIR/.claude/hooks/openspec_guard.py" \
  "$REPO_DIR/.claude/hooks/openspec_stop.py" \
  "$REPO_DIR/.codex/hooks/openspec_context.py" \
  "$REPO_DIR/.codex/hooks/openspec_router.py" \
  "$REPO_DIR/.codex/hooks/openspec_guard.py" \
  "$REPO_DIR/.codex/hooks/openspec_stop.py" \
  "$REPO_DIR/tools/openspec/healthcheck.sh"

printf '[openspec-auto] Local template permissions synced. Re-run the bootstrap install script to refresh content.\n'
