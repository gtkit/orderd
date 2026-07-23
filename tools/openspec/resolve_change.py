#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from hook_common import openspec_list, select_change


def main() -> int:
    prompt_text = " ".join(sys.argv[1:]).strip() or sys.stdin.read()
    changes = openspec_list()
    selected = select_change(prompt_text, changes)
    print(
        json.dumps(
            {
                "selected": selected,
                "activeChanges": [item.get("name") for item in changes if item.get("name")],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
