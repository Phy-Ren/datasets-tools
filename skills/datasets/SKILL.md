---
name: datasets
description: >
  Fetch research datasets by slug into /home/datasets/<slug>/ as ready-to-use
  files. A slug combines one or more sources (GitHub repo + HuggingFace dataset
  + direct URLs); archives auto-extract; an `expects` contract rejects silent
  partial fetches. Use when a task references a named dataset and the data must
  be on disk. For HuggingFace-only datasets, the huggingface plugin is the
  simpler route.
---

# Fetch datasets

Invoke as:

```bash
uv run "${CLAUDE_PLUGIN_ROOT}/dataset_tool.py" <subcommand>
```

## Subcommands

- `list` — show registered slugs with status
- `fetch <slug> [--force]` — pull every configured source, extract archives, verify `expects`, write MANIFEST
- `manifest <slug>` — print MANIFEST.md or DOWNLOAD_ME.md for a slug
- `add <slug> [--gh O/N] [--hf O/N] [--url URL]... [--expect-...]` — register a new slug

## Status (shown by `list`)

- `✓` complete — MANIFEST.md present, all listed entries on disk, contract met
- `⋯` pending — DOWNLOAD_ME.md present (manual completion required), or orphan artifacts
- (blank) missing — not fetched

## Contract `fetch` guarantees

When `fetch <slug>` exits 0, `/home/datasets/<slug>/` contains data ready for `open()`. Archives (zip, tar, tar.gz, tar.xz, tar.bz2, gzip) are extracted and deleted. If a slug was registered with `--expect-min-size-mb` or `--expect-contains`, the tool checks those before writing MANIFEST; failure writes DOWNLOAD_ME.md and exits 2.

## Exit codes

- `0` complete
- `1` error (network, filesystem, tool dep missing) — stderr explains
- `2` pending — DOWNLOAD_ME.md written; surface its contents to the user
- `64` usage error

## Registering a slug: discover every source first

Every time you register a new slug, first read the upstream README, any data/README, and any download scripts in the repo to locate every place the data actually lives. A code repo that ships only scaffolding and points to HuggingFace / Zenodo / Figshare / Dataverse / S3 for the real data is the most common failure mode — a bare `--gh` add will clone it and look successful while leaving you with no data.

Combine every source in one `add` call so fetch pulls them into the same slug directory, and pair it with an `expects` contract so fetch refuses to mint a MANIFEST when the result is short of what you asked for:

```bash
# repo + HF dataset, with a size contract that rejects a code-only clone
dataset add nmrgym \
  --gh AIMS-Lab-HKUSTGZ/NMRGym \
  --hf meaw0415/NMRGym \
  --expect-min-size-mb 50 \
  --caveat "scaffold-split NMR benchmark"
```

```bash
# HTTP archive with a required-path contract
dataset add <slug> \
  --url https://example.org/v2/data.tar.gz \
  --expect-contains data/train.csv \
  --expect-contains data/test.csv
```

Use `--expect-min-size-mb` whenever you suspect a source might return only a landing page or a code-only repo — the contract catches that failure mode at fetch time rather than downstream.

## Recovering from exit 2 (pending)

`fetch` writes DOWNLOAD_ME.md when automated completion fails. Read it (`dataset manifest <slug>` prints it) and follow the recovery path that matches the failure:

1. Sources incomplete — the data lives somewhere the registry didn't list. Read README / scripts under the slug dir to find the missing source. Then:

    ```bash
    dataset add <slug> --force --gh ... --hf ... --url ...
    dataset fetch <slug> --force
    ```

2. Bot-blocked / auth-gated / private share (0-byte, HTML body, HTTP 401/403/429) — do not retry the URL automatically (repeated hits risk an IP ban on this shared server). Ask the user to download the file in a browser and drop it into the slug directory, then run `dataset fetch <slug>` again — the tool detects the manual file and normalizes it.

3. Dataset is unpublished — the README may say "contact us" or "available upon publication". Report this to the user; the tool cannot help further.

## Slug conventions

Lowercase, hyphen-separated, descriptive. Register each meaningful variant separately (`-test`, `-small`, `-v2`, etc.).
