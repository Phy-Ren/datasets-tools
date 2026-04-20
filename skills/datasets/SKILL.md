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

## Workflow

`dataset list` shows registered slugs and whether each is cached. Run `dataset fetch <slug>` for a registered slug; files land at `/home/datasets/<slug>/` with a `MANIFEST.md`. Register an unknown slug first:

```bash
dataset add <slug> --hf user/repo          # HuggingFace
dataset add <slug> --gh owner/repo         # GitHub
dataset add <slug> --url https://...       # direct URL, repeatable
```

Record non-obvious constraints (access gating, incomplete coverage, variant selection) with `--caveat "..."` at registration time.

## On failure

The tool prints the method attempted and the registry entry to stderr and exits non-zero. Read the error. On network timeout or 5xx, one retry is reasonable. On auth / 401 / 404 / private access, report back to the user with the URL so they can intervene manually.

## Slug conventions

Lowercase, hyphen-separated, descriptive. Register each dataset variant as its own slug when the variant matters downstream.
