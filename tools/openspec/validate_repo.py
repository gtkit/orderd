#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from hook_common import OpenSpecRuntimeError, extract_json_blob, git_changed_paths, non_openspec_changes, openspec_list, run_command


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=os.getcwd())
    parser.add_argument("--ci", action="store_true")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--base-ref")
    parser.add_argument("--head-ref", default="HEAD")
    return parser.parse_args()


def resolve_base_ref(cli_value: str | None) -> str | None:
    if cli_value:
        return cli_value
    for env_name in (
        "OPENSPEC_AUTO_BASE_REF",
        "GITHUB_BASE_REF",
        "CI_MERGE_REQUEST_TARGET_BRANCH_NAME",
        "BITBUCKET_PR_DESTINATION_BRANCH",
        "CHANGE_TARGET",
        "TARGET_BRANCH",
    ):
        value = os.environ.get(env_name)
        if value:
            return value
    return None


def main() -> int:
    args = parse_args()
    repo = Path(args.repo).resolve()
    os.chdir(repo)
    base_ref = resolve_base_ref(args.base_ref)

    report = {
        "repo": str(repo),
        "activeChanges": [],
        "changedOutsideOpenSpec": [],
        "ok": True,
        "messages": [],
    }

    if args.smoke:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    try:
        changed = non_openspec_changes(
            git_changed_paths(base_ref=base_ref, head_ref=args.head_ref, cwd=repo)
        )
        report["changedOutsideOpenSpec"] = changed
    except OpenSpecRuntimeError as exc:
        report["ok"] = False
        report["messages"].append(str(exc))
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if not args.ci else 1

    try:
        changes = openspec_list(cwd=repo)
        report["activeChanges"] = [item.get("name") for item in changes if item.get("name")]
    except OpenSpecRuntimeError as exc:
        report["ok"] = False
        report["messages"].append(str(exc))
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if not args.ci else 1

    if changed and not changes:
        report["ok"] = False
        report["messages"].append("Code changed outside openspec/ but no active OpenSpec change exists.")

    if changes:
        result = run_command(["openspec", "validate", "--changes", "--strict", "--json", "--no-interactive"], cwd=repo)
        payload = extract_json_blob(result.stdout) or {}
        failed = payload.get("summary", {}).get("totals", {}).get("failed", 0)
        if result.returncode != 0 or failed:
            report["ok"] = False
            report["messages"].append("OpenSpec validation failed for one or more active changes.")

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["ok"] or not args.ci else 1


if __name__ == "__main__":
    raise SystemExit(main())
