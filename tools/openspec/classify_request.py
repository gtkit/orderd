#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from hook_common import classify_prompt


def main() -> int:
    text = " ".join(sys.argv[1:]).strip() or sys.stdin.read()
    print(json.dumps(classify_prompt(text), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
