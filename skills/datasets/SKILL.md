---
name: datasets
description: >
  Fetch research datasets by slug into the /home/datasets/ shared cache. Use
  when a task references a named dataset and the files are needed on disk.
  Handles HuggingFace datasets, public GitHub repos, and direct URL
  downloads. Fetches are idempotent and shared across users on this machine.
---

# Fetch datasets

Invoke as:

```bash
uv run "${CLAUDE_PLUGIN_ROOT}/dataset_tool.py" <subcommand>
```

The script uses PEP 723 inline metadata; uv installs dependencies automatically.

## Workflow

`dataset list` shows registered slugs and whether each is cached. If the slug you need is registered but not cached, run `dataset fetch <slug>`; it is idempotent and writes MANIFEST.md on success. If the slug is not registered, find the real source (from the paper, project page, HuggingFace, or GitHub) and register it:

```bash
dataset add <slug> --hf user/repo          # HuggingFace
dataset add <slug> --gh owner/repo         # GitHub
dataset add <slug> --url https://...       # direct URL (repeatable)
```

Record non-obvious constraints with `--caveat` when you add, for example "experimental subset only", "auth-gated, author request required", or "incomplete, evaluation split missing".

Then `dataset fetch <slug>`. On success the files land at `/home/datasets/<slug>/` along with a generated MANIFEST.md recording source, size, fetch time, and any caveat.

## When fetch fails

The tool prints the method attempted and the registry entry to stderr, then exits non-zero. Do not retry blindly or pretend success. Read the error, and if it is an auth or private-access failure, report back to the user with the URL and the fact that manual intervention is needed. If it is a transient network error, one retry is reasonable before reporting.

## Slug conventions

Lowercase, hyphen-separated, descriptive. Examples: `nmrgym`, `alberts-2024-ir`, `geom-drugs`, `vermeyen-2023-vcd`. If a dataset ships variants that matter for downstream use (full vs. scaffold-split subset, weights vs. training data), register each as its own slug (`nmrgym`, `nmrgym-scaffold`) rather than nesting.

## Limitations

Public sources only. Auth-gated datasets (author request, ChemRxiv supplementary behind credentials, gated HuggingFace repos) will fail at fetch time; register them with a caveat and report back to the user.
