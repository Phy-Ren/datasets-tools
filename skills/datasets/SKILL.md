---
name: datasets
description: >
  Fetch non-HuggingFace research datasets by slug into the /home/datasets/
  shared cache. Use when a task references a named dataset hosted on github
  or a direct download URL and the files are needed on disk. Fetches are
  idempotent and shared across users on this machine. HuggingFace-hosted
  datasets are out of scope; use the huggingface plugin for those.
---

# Fetch datasets

Invoke as:

```bash
uv run "${CLAUDE_PLUGIN_ROOT}/dataset_tool.py" <subcommand>
```

## Workflow

`dataset list` shows registered slugs and whether each is cached. Run `dataset fetch <slug>` for a registered slug; files land at `/home/datasets/<slug>/` with a `MANIFEST.md`. Register an unknown slug first:

```bash
dataset add <slug> --gh owner/repo         # GitHub
dataset add <slug> --url https://...       # direct URL, repeatable
```

Record non-obvious constraints via `--caveat "..."` at registration time (for example, access gating that the tool cannot detect upfront).

## On failure

The tool prints the method attempted and the registry entry to stderr and exits non-zero. Read the error. On network timeout or 5xx, retry at most once. Otherwise report back to the user with the URL for manual intervention.

## Slug conventions

Lowercase, hyphen-separated, descriptive. Register each dataset variant as its own slug when the variant matters downstream.
