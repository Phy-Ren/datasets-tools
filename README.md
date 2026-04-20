# datasets-tools

Fetch research datasets into `/home/datasets/<slug>/` with a MANIFEST. Shared local cache across users on this machine.

## Setup

```bash
sudo mkdir -p /home/datasets && sudo chmod 2777 /home/datasets
```

Then register the plugin in `~/.claude/settings.json`:

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

## Usage

```bash
uv run ~/datasets-tools/dataset_tool.py list
uv run ~/datasets-tools/dataset_tool.py add nmrgym --hf xiong-group/NMRGym --caveat "scaffold-split release"
uv run ~/datasets-tools/dataset_tool.py fetch nmrgym
uv run ~/datasets-tools/dataset_tool.py manifest nmrgym
```

Subcommands: `fetch | list | add | manifest`. Registry lives at `/home/datasets/registry.yaml`. Each fetched dataset gets `/home/datasets/<slug>/MANIFEST.md`.

## Sources

- HuggingFace datasets: `--hf user/repo`, optional `--hf-allow '*.csv'`
- GitHub: `--gh owner/name`, optional `--gh-ref main`
- Direct URL: `--url ...` (repeatable)

Fetches are idempotent (re-run is a no-op unless `--force`) and concurrent-safe (per-slug `flock`). Failures print the attempted method and registry entry; no silent retries.
