# datasets-tools

Fetch research datasets into `/home/datasets/<slug>/` as ready-to-use files. A slug may combine multiple sources (GitHub + HuggingFace + direct URLs) into one directory. Archives auto-extract. An optional `expects` contract rejects silent partials.

## Setup

```bash
sudo mkdir -p /home/datasets && sudo chmod 2777 /home/datasets
```

Register the plugin in `~/.claude/settings.json`:

```json
{
  "extraKnownMarketplaces": {
    "datasets-tools": {
      "source": { "source": "directory", "path": "/home/xingyu/datasets-tools" }
    }
  },
  "enabledPlugins": {
    "datasets-tools@datasets-tools": true
  }
}
```

HuggingFace support shells out to `huggingface-cli`. Install once per machine:

```bash
pip install -U 'huggingface_hub[cli]'
```

## Usage

```bash
# register a multi-source slug with a size contract
uv run ~/datasets-tools/dataset_tool.py add nmrgym \
  --gh AIMS-Lab-HKUSTGZ/NMRGym --hf meaw0415/NMRGym \
  --expect-min-size-mb 50

# pull everything
uv run ~/datasets-tools/dataset_tool.py fetch nmrgym

# check state
uv run ~/datasets-tools/dataset_tool.py list
uv run ~/datasets-tools/dataset_tool.py manifest nmrgym

# regenerate MANIFEST from what's already on disk — no refetch
# (use after editing the registry or upgrading the tool)
uv run ~/datasets-tools/dataset_tool.py remanifest nmrgym
```

## Pipeline

`fetch <slug>` runs:

1. Download — git clone, `huggingface-cli download`, and HTTP streaming (each configured source, merged into the same target dir). HTTP writes to `<file>.part` and renames on success.
2. Sanity — non-zero size, non-HTML body, ZIP central-directory valid.
3. Extract — recursively unpack archives in place, delete archives (≤ 3 nested levels).
4. Contract — verify `expects.min_size_mb` and `expects.contains`.
5. Manifest — write MANIFEST.md with top-level entries and sizes. The header records `source` (registry intent) and `method` (what actually ran this invocation — e.g. `source: http, method: manual` when an agent drops files past a WAF); `elapsed` is annotated with what it measures (`fetch`, `promote only`, `remanifest`); `requires` lists load-time Python deps (declared + auto-detected).

Any failure during steps 1–4 writes `DOWNLOAD_ME.md` with recovery steps and exits 2 instead of MANIFEST-ing a partial directory.

## Registry schema

```yaml
<slug>:
  gh_repo: OWNER/NAME        # optional, cloned first
  gh_ref: BRANCH/TAG/COMMIT  # optional
  hf_dataset: OWNER/NAME     # optional, downloaded via huggingface-cli
  hf_subdir: DIR             # optional, placed under this relative subdir
  hf_revision: REV           # optional
  url: URL                   # optional singular
  urls: [URL, ...]           # optional plural (use when >1)
  expects:                   # optional contract
    min_size_mb: 50
    contains: [data/train.csv]
  requires: [pandas, rdkit]  # optional, Python deps needed to load the files
  caveat: post-fetch note about the data (license / citation / loading quirks)
  source: derived "github+hf+http" label (read-only display)
```

`requires` is also auto-detected from serialized-object opcodes in `.pkl` files
and from file extensions (`.parquet` → pyarrow, `.h5` → h5py, `.npy` → numpy,
`.pt` → torch, `.safetensors` → safetensors). Declared entries take precedence;
auto-detected entries are suffixed `(auto)` in MANIFEST.

`caveat` is for post-fetch realities. Fetch-time obstacles (bot challenges,
private shares, "manual download needed") belong in the pending flow — they
get surfaced in `DOWNLOAD_ME.md` while a slug is pending, then cleared when
MANIFEST is written. Any such sentences in `caveat` are auto-pruned at render
time so MANIFEST never contradicts ground truth.

At least one of `gh_repo` / `hf_dataset` / `url(s)` must be present.

## Exit codes

| code | meaning |
|---|---|
| 0 | complete |
| 1 | error (network, filesystem, bad archive, missing tool) |
| 2 | pending — DOWNLOAD_ME.md written, manual completion required |
| 64 | usage |

## Status in `list`

- `✓` complete — MANIFEST present, all listed entries on disk
- `⋯` pending — DOWNLOAD_ME present, or interrupted fetch orphans
- (blank) missing

Fetches are idempotent (re-run is a no-op unless `--force`) and concurrent-safe (per-slug `flock`).
