#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools" / "openspec"))

from hook_common import build_router_context, emit_response, read_hook_payload, summarize_payload


if __name__ == "__main__":
    payload = read_hook_payload()
    text = summarize_payload(payload)
    emit_response(event_name="UserPromptSubmit", additional_context=build_router_context(text))
