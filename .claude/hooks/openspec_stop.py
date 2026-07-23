#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "openspec"))

from hook_common import emit_response, read_hook_payload, should_block_completion, summarize_payload


if __name__ == "__main__":
    payload = read_hook_payload()
    text = summarize_payload(payload)
    blocked, reason = should_block_completion(text)
    if blocked:
        emit_response(event_name="Stop", continue_value=False, stop_reason=reason)
    else:
        emit_response(event_name="Stop")
