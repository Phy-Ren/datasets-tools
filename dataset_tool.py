#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pyyaml>=6.0",
#   "requests>=2.31",
# ]
# ///
"""dataset — fetch non-HuggingFace research datasets into /home/datasets/<slug>/.

Sources: github repos, direct URL downloads. For HuggingFace use the
huggingface plugin instead.

subcommands: fetch | list | add | manifest
"""
from __future__ import annotations

import argparse
import fcntl
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

ROOT = Path("/home/datasets")
REGISTRY = ROOT / "registry.yaml"


def load_registry() -> dict:
    if not REGISTRY.exists():
        return {}
    return yaml.safe_load(REGISTRY.read_text()) or {}


def save_registry(reg: dict) -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    tmp = REGISTRY.with_suffix(".yaml.tmp")
    tmp.write_text(yaml.safe_dump(reg, sort_keys=True, allow_unicode=True))
    tmp.replace(REGISTRY)


def slug_dir(slug: str) -> Path:
    return ROOT / slug


def acquire_lock(name: str):
    ROOT.mkdir(parents=True, exist_ok=True)
    fp = open(ROOT / f".{name}.lock", "w")
    fcntl.flock(fp, fcntl.LOCK_EX)
    return fp


def fetch_github(entry: dict, target: Path) -> dict:
    url = f"https://github.com/{entry['gh_repo']}.git"
    ref = entry.get("gh_ref")
    cmd = ["git", "clone", url, str(target)]
    if not ref:
        cmd[2:2] = ["--depth", "1"]
    subprocess.run(cmd, check=True)
    if ref:
        subprocess.run(["git", "-C", str(target), "checkout", ref], check=True)
    return {"method": " ".join(cmd[:-2]), "repo": entry["gh_repo"], "ref": ref}


def fetch_http(entry: dict, target: Path) -> dict:
    urls = entry.get("urls") or [entry["url"]]
    target.mkdir(parents=True, exist_ok=True)
    files = []
    for url in urls:
        name = url.rsplit("/", 1)[-1] or "download"
        http_download(url, target / name)
        files.append(name)
    return {"method": "http", "urls": urls, "files": files}


def http_download(url: str, dest: Path, tries: int = 3) -> None:
    for i in range(tries):
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with dest.open("wb") as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk)
            return
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(2**i)


FETCHERS = {
    "github": fetch_github,
    "http": fetch_http,
}


def render_manifest(slug: str, entry: dict, meta: dict, elapsed: float, size: int) -> str:
    parts = [
        f"# {slug}",
        "",
        f"- fetched: {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        f"- source: {entry['source']}",
        f"- size: {size / 1e6:.1f} MB",
        f"- elapsed: {elapsed:.1f}s",
        f"- fetch_meta: {json.dumps(meta, ensure_ascii=False)}",
    ]
    if caveat := entry.get("caveat"):
        parts += ["", "## caveat", "", caveat]
    parts += [
        "",
        "## registry entry",
        "",
        "```yaml",
        yaml.safe_dump({slug: entry}, sort_keys=True, allow_unicode=True).rstrip(),
        "```",
        "",
    ]
    return "\n".join(parts)


def cmd_fetch(args) -> int:
    reg = load_registry()
    entry = reg.get(args.slug)
    if entry is None:
        print(f"error: '{args.slug}' not in registry (use 'dataset add' first)", file=sys.stderr)
        return 2
    source = entry.get("source")
    if source not in FETCHERS:
        print(f"error: unknown source '{source}' (known: {list(FETCHERS)})", file=sys.stderr)
        return 2

    target = slug_dir(args.slug)
    manifest = target / "MANIFEST.md"
    if manifest.exists() and not args.force:
        print(f"already cached: {target} (--force to refetch)")
        return 0

    lock = acquire_lock(args.slug)
    try:
        if target.exists():
            shutil.rmtree(target)
        t0 = time.time()
        try:
            meta = FETCHERS[source](entry, target)
        except Exception as e:
            shutil.rmtree(target, ignore_errors=True)
            print(f"fetch failed: {e}", file=sys.stderr)
            print(
                "registry entry:\n"
                + yaml.safe_dump({args.slug: entry}, allow_unicode=True, sort_keys=True),
                file=sys.stderr,
            )
            return 1
        elapsed = time.time() - t0
        size = sum(p.stat().st_size for p in target.rglob("*") if p.is_file())
        manifest.write_text(render_manifest(args.slug, entry, meta, elapsed, size))
        print(f"ok: {target} ({size / 1e6:.1f} MB, {elapsed:.1f}s)")
        return 0
    finally:
        lock.close()


def cmd_list(args) -> int:
    reg = load_registry()
    if not reg:
        print("(empty)")
        return 0
    for slug in sorted(reg):
        cached = "✓" if (slug_dir(slug) / "MANIFEST.md").exists() else " "
        source = reg[slug].get("source", "?")
        print(f"{cached}  {slug:<32s}  {source}")
    return 0


def cmd_add(args) -> int:
    provided = [name for name, val in (("--gh", args.gh), ("--url", args.url)) if val]
    if len(provided) != 1:
        got = ", ".join(provided) or "none"
        print(f"error: specify exactly one of --gh / --url (got: {got})", file=sys.stderr)
        return 2

    lock = acquire_lock("registry")
    try:
        reg = load_registry()
        if args.slug in reg and not args.force:
            print(f"error: '{args.slug}' exists (--force to overwrite)", file=sys.stderr)
            return 2
        entry: dict = {}
        if args.gh:
            entry["source"] = "github"
            entry["gh_repo"] = args.gh
            if args.gh_ref:
                entry["gh_ref"] = args.gh_ref
        elif args.url:
            entry["source"] = "http"
            if len(args.url) == 1:
                entry["url"] = args.url[0]
            else:
                entry["urls"] = args.url
        if args.caveat:
            entry["caveat"] = args.caveat
        reg[args.slug] = entry
        save_registry(reg)
        print(f"added: {args.slug}")
        return 0
    finally:
        lock.close()


def cmd_manifest(args) -> int:
    path = slug_dir(args.slug) / "MANIFEST.md"
    if not path.exists():
        print(f"not cached: {args.slug}", file=sys.stderr)
        return 1
    print(path.read_text(), end="")
    return 0


def main():
    p = argparse.ArgumentParser(prog="dataset", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    f = sub.add_parser("fetch", help="fetch a registered dataset")
    f.add_argument("slug")
    f.add_argument("--force", action="store_true")
    f.set_defaults(func=cmd_fetch)

    l = sub.add_parser("list", help="list registered datasets")
    l.set_defaults(func=cmd_list)

    a = sub.add_parser("add", help="register a new dataset")
    a.add_argument("slug")
    a.add_argument("--gh", metavar="OWNER/NAME", help="GitHub repo")
    a.add_argument("--gh-ref", metavar="REF", help="git branch / tag / commit")
    a.add_argument("--url", action="append", metavar="URL", help="direct URL (repeatable)")
    a.add_argument("--caveat", metavar="TEXT", help="free-form note saved with registry entry")
    a.add_argument("--force", action="store_true")
    a.set_defaults(func=cmd_add)

    m = sub.add_parser("manifest", help="print MANIFEST.md for a cached dataset")
    m.add_argument("slug")
    m.set_defaults(func=cmd_manifest)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
