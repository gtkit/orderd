#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "openspec"))

from hook_common import emit_response, read_hook_payload, should_allow_edit, summarize_payload


if __name__ == "__main__":
    payload = read_hook_payload()
    text = summarize_payload(payload)
    allowed, reason = should_allow_edit(text)
    if allowed:
        emit_response(event_name="PreToolUse", additional_context=f"OpenSpec guard: {reason}")
    else:
        emit_response(
            event_name="PreToolUse",
            permission_decision="deny",
            additional_context=f"OpenSpec guard blocked this edit: {reason} Prepare or choose the correct OpenSpec change first.",
        )
