#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/env.sh"

REPO_DIR="${1:-$(openspec_repo_root)}"
REPO_DIR="$(cd "$REPO_DIR" && pwd)"

printf '[openspec-auto] healthcheck repo=%s\n' "$REPO_DIR"

openspec_require_cmd python3
RUNNER="$(openspec_resolve_runner)"
NODE_BIN="$(openspec_resolve_node_bin)"

node_version="$("$NODE_BIN" -p 'process.versions.node' 2>/dev/null || true)"
python3 - "$node_version" <<'PY'
import sys
from itertools import zip_longest

required = (20, 19, 0)
raw = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else ""
if not raw:
    print("[openspec-auto][error] Node.js version could not be detected", file=sys.stderr)
    sys.exit(1)
try:
    parts = tuple(int(p.split("-")[0]) for p in raw.split(".")[:3])
except (ValueError, IndexError):
    print("[openspec-auto][error] Node.js version format not recognized: " + raw, file=sys.stderr)
    sys.exit(1)
for left, right in zip_longest(parts, required, fillvalue=0):
    if left > right:
        sys.exit(0)
    if left < right:
        print("[openspec-auto][error] Node.js must be >= 20.19.0", file=sys.stderr)
        sys.exit(1)
sys.exit(0)
PY

(
  cd "$REPO_DIR"
  OPENSPEC_TELEMETRY=0 bash -lc "$RUNNER list --json >/dev/null"
)

# Verify installation completeness: hook files, config registrations, managed blocks
python3 - "$REPO_DIR" <<'PY'
import json
import sys
from pathlib import Path

repo = Path(sys.argv[1])
errors = []

# Check critical hook files exist (Claude + Codex + shared tools)
for hook in (
    ".claude/hooks/openspec_context.py",
    ".claude/hooks/openspec_router.py",
    ".claude/hooks/openspec_guard.py",
    ".claude/hooks/openspec_stop.py",
    ".codex/hooks/openspec_context.py",
    ".codex/hooks/openspec_router.py",
    ".codex/hooks/openspec_guard.py",
    ".codex/hooks/openspec_stop.py",
    "tools/openspec/hook_common.py",
):
    if not (repo / hook).is_file():
        errors.append(f"missing file: {hook}")

# Check skill definitions
for skill in (
    ".claude/skills/openspec-auto/SKILL.md",
    ".agents/skills/openspec-auto/SKILL.md",
):
    if not (repo / skill).is_file():
        errors.append(f"missing file: {skill}")

# Check Claude settings.json has openspec hook registrations
claude_settings = repo / ".claude" / "settings.json"
if claude_settings.exists():
    try:
        hooks = json.loads(claude_settings.read_text()).get("hooks", {})
        for event in ("SessionStart", "UserPromptSubmit", "PreToolUse", "Stop"):
            found = any(
                "openspec_" in h.get("command", "")
                for entry in hooks.get(event, [])
                for h in entry.get("hooks", [])
            )
            if not found:
                errors.append(f"settings.json missing openspec hook for {event}")
    except (json.JSONDecodeError, AttributeError) as exc:
        errors.append(f"settings.json parse error: {exc}")
else:
    errors.append("missing file: .claude/settings.json")

# Check Codex hooks.json has openspec hook registrations
codex_hooks = repo / ".codex" / "hooks.json"
if codex_hooks.exists():
    try:
        hooks = json.loads(codex_hooks.read_text()).get("hooks", {})
        for event in ("SessionStart", "UserPromptSubmit", "PreToolUse", "Stop"):
            found = any(
                "openspec_" in h.get("command", "")
                for entry in hooks.get(event, [])
                for h in entry.get("hooks", [])
            )
            if not found:
                errors.append(f"hooks.json missing openspec hook for {event}")
    except (json.JSONDecodeError, AttributeError) as exc:
        errors.append(f"hooks.json parse error: {exc}")
else:
    errors.append("missing file: .codex/hooks.json")

# Check managed blocks in rule files
for name in ("AGENTS.md", "CLAUDE.md"):
    path = repo / name
    if path.exists():
        if "OPENSPEC-AUTO:START" not in path.read_text():
            errors.append(f"{name} missing OPENSPEC-AUTO managed block")
    else:
        errors.append(f"missing file: {name}")

if errors:
    for e in errors:
        print(f"[openspec-auto][error] {e}", file=sys.stderr)
    sys.exit(1)

print("[openspec-auto] installation integrity verified")
PY

# Verify Codex user config has codex_hooks enabled (warning, not fatal)
if command -v codex >/dev/null 2>&1; then
  python3 - <<'PY'
import re
import sys
from pathlib import Path

config_path = Path.home() / ".codex" / "config.toml"
if not config_path.exists():
    print("[openspec-auto][warn] ~/.codex/config.toml not found — Codex repo-local hooks will NOT fire", file=sys.stderr)
    print("[openspec-auto][warn] Fix: mkdir -p ~/.codex && printf '[features]\\ncodex_hooks = true\\n' > ~/.codex/config.toml", file=sys.stderr)
    sys.exit(0)

text = config_path.read_text()
if not re.search(r"^\s*codex_hooks\s*=\s*true\b", text, re.M):
    print("[openspec-auto][warn] ~/.codex/config.toml exists but codex_hooks is not enabled — Codex repo-local hooks will NOT fire", file=sys.stderr)
    print("[openspec-auto][warn] Fix: add 'codex_hooks = true' under [features] in ~/.codex/config.toml", file=sys.stderr)
    sys.exit(0)

print("[openspec-auto] codex_hooks = true verified in ~/.codex/config.toml")
PY
fi

if command -v claude >/dev/null 2>&1; then
  printf '[openspec-auto] claude=%s\n' "$(claude -v 2>/dev/null || printf 'unknown')"
else
  printf '[openspec-auto] claude=missing (warning)\n'
fi

if command -v codex >/dev/null 2>&1; then
  printf '[openspec-auto] codex=present\n'
else
  printf '[openspec-auto] codex=missing (warning)\n'
fi

printf '[openspec-auto] healthcheck passed\n'
