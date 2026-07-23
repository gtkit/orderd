#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "openspec"))

from hook_common import build_session_context, emit_response


if __name__ == "__main__":
    emit_response(event_name="SessionStart", additional_context=build_session_context())
