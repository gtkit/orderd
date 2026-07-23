---
name: openspec-auto
description: Use when the repository uses OpenSpec and the user asks for a feature, bug fix, refactor, or any code change that affects runtime behavior, interfaces, data shape, or product behavior, and the workflow should happen automatically without asking the user to type OpenSpec commands manually.
---

# OpenSpec Auto

OpenSpec is the default execution layer for behavior-changing work in this repository.

## Use This For

- New features
- Bug fixes
- Refactors that affect behavior or interfaces
- API, DTO, database, migration, or frontend behavior changes
- Runtime config changes

Do not use this for:
- Pure explanation or exploration
- Read-only review
- Docs-only edits unless the docs are OpenSpec artifacts

## Default Flow

1. Check active changes:
   - Run `openspec list --json`
2. Resolve the change:
   - If the user names a change, use it.
   - If exactly one active change clearly matches, continue it.
   - If multiple active changes could match, ask the user which one to use.
   - If no active change exists, create one from the request.
3. Create a new change when needed:
   - Derive a kebab-case change name from the request.
   - Run `openspec new change "<name>"`
4. Make the change apply-ready before application edits:
   - Run `openspec status --change "<name>" --json`
   - For each artifact with `status: "ready"`, run `openspec instructions <artifact-id> --change "<name>" --json`
   - Create the artifact file from the provided template and instructions
   - Re-run `openspec status --change "<name>" --json`
   - Stop artifact creation only when every artifact in `applyRequires` is done
5. Implement through the change:
   - Read the artifact context first
   - Follow the tasks in order
   - Keep the task checklist current
6. Verify before completion:
   - Run `openspec validate "<name>" --type change --strict --json --no-interactive`
   - Run the repository's own tests and validations
   - If validation exposes a design or scope gap, update the OpenSpec artifacts instead of guessing

## Guardrails

- Do not silently choose among multiple plausible active changes
- Do not start application edits before the change is apply-ready unless the user explicitly skips OpenSpec
- Do not ask the user to manually type `/opsx:*` or raw `openspec` commands unless they asked for manual control
- Treat OpenSpec CLI output as the source of truth
