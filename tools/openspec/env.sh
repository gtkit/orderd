#!/usr/bin/env bash
set -euo pipefail

export OPENSPEC_TELEMETRY="${OPENSPEC_TELEMETRY:-0}"

openspec_repo_root() {
  if git rev-parse --show-toplevel >/dev/null 2>&1; then
    git rev-parse --show-toplevel
  else
    pwd
  fi
}

openspec_require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    printf '[openspec-auto][error] missing command: %s\n' "$1" >&2
    return 1
  }
}

openspec_version_ok() {
  python3 - "$1" <<'PY'
import sys
from itertools import zip_longest

required = (20, 19, 0)
try:
    parts = tuple(int(p.split("-")[0]) for p in sys.argv[1].split(".")[:3])
except (ValueError, IndexError):
    sys.exit(1)
for left, right in zip_longest(parts, required, fillvalue=0):
    if left > right:
        sys.exit(0)
    if left < right:
        sys.exit(1)
sys.exit(0)
PY
}

openspec_resolve_node_bin() {
  python3 - <<'PY'
import glob
import os
import shutil
import subprocess
import sys

def version_ok(version: str) -> bool:
    try:
        parts = tuple(int(p.split("-")[0]) for p in version.split(".")[:3])
    except (ValueError, IndexError):
        return False
    required = (20, 19, 0)
    padded = parts + (0,) * (3 - len(parts))
    return padded >= required

def node_version(path: str) -> str | None:
    try:
        result = subprocess.run([path, "-p", "process.versions.node"], text=True, capture_output=True, check=False)
    except OSError:
        return None
    version = result.stdout.strip()
    return version if result.returncode == 0 and version else None

candidates = []
for item in [os.environ.get("NODE_BIN"), shutil.which("node")]:
    if item:
        candidates.append(item)
candidates.extend(glob.glob(os.path.expanduser("~/.nvm/versions/node/*/bin/node")))

best = None
best_version = ()
for candidate in candidates:
    version = node_version(candidate)
    if not version or not version_ok(version):
        continue
    try:
        key = tuple(int(p.split("-")[0]) for p in version.split(".")[:3])
    except (ValueError, IndexError):
        continue
    if key > best_version:
        best = candidate
        best_version = key

if not best:
    sys.exit(1)

print(best)
PY
}

openspec_resolve_runner() {
  local node_bin openspec_path
  node_bin="$(openspec_resolve_node_bin)"
  openspec_path="${OPENSPEC_BIN:-$(command -v openspec || true)}"

  [[ -n "$openspec_path" ]] || {
    printf '[openspec-auto][error] openspec command not found\n' >&2
    return 1
  }

  if "$openspec_path" --version >/dev/null 2>&1; then
    printf '%s\n' "$openspec_path"
    return 0
  fi

  printf '%s %s\n' "$node_bin" "$openspec_path"
}
