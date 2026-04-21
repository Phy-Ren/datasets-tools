#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pyyaml>=6.0",
#   "requests>=2.31",
# ]
# ///
"""dataset — fetch research datasets as ready-to-use files in /home/datasets/<slug>/.

A slug may combine multiple sources (github + huggingface + direct URLs). Fetch
pulls every configured source into the same target directory, recursively
extracts archives, runs auto-resolve for second-stage data sources referenced
in github clones, and writes MANIFEST.md only if the optional `expects`
contract (min size, required paths) is satisfied. On contract violation or
bot-blocked sources, the tool records slug state in `<slug>/.pending.json` and
aggregates all pending items into a single top-level `/home/datasets/DOWNLOAD_ME.md`
so the user sees every outstanding action in one file. The aggregate rebuilds
on every fetch and self-deletes when no items remain pending.

Exit codes:
  0  complete (MANIFEST.md written)
  1  error (network, filesystem, bad archive, tool dependency missing)
  2  pending (.pending.json written; see /home/datasets/DOWNLOAD_ME.md)
 64  usage error (unknown slug, bad flags)

HuggingFace support is built-in; it shells out to `huggingface-cli`. This tool
owns orchestration only; the huggingface plugin owns the download itself.
"""
from __future__ import annotations

import argparse
import bz2
import fcntl
import gzip
import json
import lzma
import re
import shutil
import subprocess
import sys
import tarfile
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

import requests
import yaml

ROOT = Path("/home/datasets")
REGISTRY = ROOT / "registry.yaml"
MANIFEST = "MANIFEST.md"
PENDING_META = ".pending.json"
TOP_DOWNLOAD_ME = ROOT / "DOWNLOAD_ME.md"
_LEGACY_DOWNLOAD_ME = "DOWNLOAD_ME.md"  # slug-level file from pre-aggregate format
MAX_EXTRACT_DEPTH = 3
CHUNK = 1 << 20

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_PENDING = 2
EXIT_USAGE = 64


class FetchError(Exception):
    """Tool- or network-level failure reportable to the user."""


class NeedsManual(Exception):
    """Source requires manual download (bot challenge / login / private share)."""

    def __init__(self, url: str, reason: str):
        super().__init__(f"{reason}: {url}")
        self.url = url
        self.reason = reason


# ---------- caveat staleness / load-time dep detection ----------

# Fetch-time obstacles (bot challenges, private shares, landing pages, …) are
# the kind of caveat that becomes *false* the moment a MANIFEST gets written —
# either because the fetch succeeded or because a manual drop was promoted. We
# strip those sentences on render so MANIFEST stays truthful; the same guidance
# still lives in DOWNLOAD_ME while the slug is pending.
_STALE_CAVEAT_WORDS = re.compile(
    r"\b(?:manual\s+download|bot[-\s]*(?:block|challeng)\w*|"
    r"landing\s+page|private\s+share|needs?\s+manual|"
    r"fetch\s+saves\s+html|login[-\s]*gated|"
    r"not\s+direct\s+file|requires\s+login)\b",
    re.IGNORECASE,
)


def filter_stale_caveat(text: str | None) -> str:
    if not text:
        return ""
    parts = re.split(r"\s*(?:;|—|\.)\s+", text.strip())
    kept = [p.rstrip(".; ").strip() for p in parts
            if p.strip() and not _STALE_CAVEAT_WORDS.search(p)]
    return "; ".join(kept)


# Stdlib top-level modules that show up in serialized GLOBAL ops but are never
# a real install-time dep. Missing entries cause a noisy but harmless requires
# line; false positives would mislead the user into `pip install builtins`.
_REQUIRES_SKIP_TOPMODS = frozenset({
    "builtins", "__builtin__", "copy_reg", "copyreg", "__main__",
    "collections", "array", "datetime", "pathlib", "uuid",
    "decimal", "fractions", "io", "os", "sys", "json",
    "re", "typing", "dataclasses", "enum", "functools", "itertools",
    "abc", "numbers", "weakref", "struct",
})

_SERIALIZED_UNICODE_OPS = frozenset({
    "SHORT_BINUNICODE", "BINUNICODE", "BINUNICODE8",
    "UNICODE", "SHORT_BINSTRING", "BINSTRING", "STRING",
})

# File extensions that imply a specific loader dep even without an opcode scan.
_EXT_REQUIRES = {
    ".parquet": ("pyarrow",),
    ".feather": ("pyarrow",),
    ".h5": ("h5py",),
    ".hdf5": ("h5py",),
    ".npy": ("numpy",),
    ".npz": ("numpy",),
    ".pt": ("torch",),
    ".pth": ("torch",),
    ".safetensors": ("safetensors",),
}


def _sniff_serialized_requires(target: Path, *, max_files: int = 20,
                               max_bytes: int = 5 * 1024 * 1024) -> set[str]:
    """Walk the opcode stream of each .pkl and collect non-stdlib top-level
    modules referenced by GLOBAL / STACK_GLOBAL. Uses pickletools.genops, which
    only parses opcodes — no deserialization, no code execution."""
    import pickletools
    from io import BytesIO
    mods: set[str] = set()
    for path in sorted(target.rglob("*.pkl"))[:max_files]:
        try:
            size = path.stat().st_size
            with path.open("rb") as f:
                buf = f.read(min(size, max_bytes))
            # Cheap, robust heuristic: only accept candidates that contain a
            # dot. Any serialized class that survives round-tripping is
            # reconstructed through a dotted path at least once (numpy uses
            # `numpy.core.multiarray._reconstruct`, pandas uses
            # `pandas.core.frame.DataFrame`, rdkit uses `rdkit.Chem.rdchem.Mol`
            # …), so the top-level name is recoverable even for packages whose
            # canonical short name has no dot. Filtering by dot rejects dict
            # keys, field names, and other unicode pushes that get routed
            # through GLOBAL / STACK_GLOBAL slots when a simple stack
            # simulation would misattribute them.
            stack: list[str] = []
            for op, arg, _pos in pickletools.genops(BytesIO(buf)):
                name = op.name
                if name in _SERIALIZED_UNICODE_OPS:
                    if isinstance(arg, bytes):
                        stack.append(arg.decode("utf-8", "ignore"))
                    elif isinstance(arg, str):
                        stack.append(arg)
                elif name == "GLOBAL" and isinstance(arg, str):
                    head = arg.split(None, 1)[0] if arg else ""
                    if "." in head:
                        mods.add(head)
                elif name == "STACK_GLOBAL":
                    if len(stack) >= 2:
                        candidate = stack[-2]
                        if "." in candidate:
                            mods.add(candidate)
                        stack = stack[:-2]
        except Exception:
            continue
    return {m.split(".", 1)[0] for m in mods if m}


def detect_requires(target: Path) -> list[str]:
    """Heuristic load-time deps: serialized-object opcodes + file extensions."""
    mods = _sniff_serialized_requires(target)
    for p in target.rglob("*"):
        if p.is_file():
            for dep in _EXT_REQUIRES.get(p.suffix.lower(), ()):
                mods.add(dep)
    return sorted(m for m in mods if m and m not in _REQUIRES_SKIP_TOPMODS)


# ---------- registry ----------


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


def active_sources(entry: dict) -> list[str]:
    """Ordered list of source kinds present in an entry (github first — it needs
    an empty target dir; hf and http merge into the existing dir afterward)."""
    out = []
    if entry.get("gh_repo"):
        out.append("github")
    if entry.get("hf_dataset"):
        out.append("hf")
    if entry.get("url") or entry.get("urls"):
        out.append("http")
    return out


def source_label(entry: dict) -> str:
    return "+".join(active_sources(entry)) or "?"


# ---------- HTTP download ----------


_FILENAME_SAFE = re.compile(r"[^\w.\-]+")


def _sanitize(name: str) -> str:
    name = name.strip().strip("/")
    return _FILENAME_SAFE.sub("_", name)[:200] or "download"


def _filename_from_response(r: requests.Response, url: str) -> str:
    cd = r.headers.get("content-disposition", "")
    if cd:
        msg = EmailMessage()
        msg["content-disposition"] = cd
        fn = msg.get_filename()
        if fn:
            return _sanitize(fn)
    tail = url.rsplit("/", 1)[-1].split("?", 1)[0]
    return _sanitize(tail) if tail else "download"


def _looks_like_html(path: Path) -> bool:
    try:
        with path.open("rb") as f:
            head = f.read(1024).lstrip().lower()
    except OSError:
        return False
    return head.startswith((b"<!doctype html", b"<html"))


def http_download(url: str, target: Path, *, tries: int = 3) -> tuple[Path, int]:
    """Stream url to target/<filename>.part, rename on success. Return (final, bytes)."""
    for attempt in range(tries):
        try:
            with requests.get(url, stream=True, timeout=60, allow_redirects=True) as r:
                status = r.status_code
                ctype = r.headers.get("content-type", "").lower()
                if status in (401, 403, 429):
                    raise NeedsManual(url, f"HTTP {status}")
                if 500 <= status < 600:
                    if attempt < tries - 1:
                        time.sleep(2**attempt)
                        continue
                    raise FetchError(f"HTTP {status} after {tries} tries: {url}")
                if not (200 <= status < 300):
                    raise FetchError(f"HTTP {status}: {url}")
                filename = _filename_from_response(r, url)
                final = target / filename
                part = target / (filename + ".part")
                size = 0
                with part.open("wb") as f:
                    for chunk in r.iter_content(CHUNK):
                        f.write(chunk)
                        size += len(chunk)
                if size == 0:
                    part.unlink(missing_ok=True)
                    raise NeedsManual(url, f"0-byte response (HTTP {status})")
                if "text/html" in ctype and _looks_like_html(part):
                    part.unlink(missing_ok=True)
                    raise NeedsManual(url, "HTML response (likely bot challenge or landing page)")
                part.rename(final)
                return final, size
        except NeedsManual:
            raise
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt == tries - 1:
                raise FetchError(f"network error after {tries} tries: {url} ({e.__class__.__name__})")
            time.sleep(2**attempt)
        except requests.RequestException as e:
            raise FetchError(f"request failed: {url} ({e})")
    raise FetchError(f"exhausted retries: {url}")


# ---------- archive detection & extraction ----------


_MAGIC = [
    (b"PK\x03\x04", "zip"),
    (b"PK\x05\x06", "zip"),
    (b"\x1f\x8b", "gzip"),
    (b"BZh", "bzip2"),
    (b"\xfd7zXZ\x00", "xz"),
]


def sniff_archive(path: Path) -> str | None:
    try:
        with path.open("rb") as f:
            head = f.read(8)
            for magic, kind in _MAGIC:
                if head.startswith(magic):
                    return kind
            f.seek(257)
            if f.read(5) == b"ustar":
                return "tar"
    except OSError:
        return None
    return None


def _extract_single_compressed(path: Path, opener) -> None:
    stem = path.with_suffix("").name if path.suffix in (".gz", ".bz2", ".xz") else path.name + ".out"
    out = path.parent / stem
    with opener(path, "rb") as src, out.open("wb") as dst:
        shutil.copyfileobj(src, dst, length=CHUNK)


def extract_archive(path: Path) -> bool:
    """Extract archive into its parent dir, then delete the archive. Return True if extracted."""
    kind = sniff_archive(path)
    if kind is None:
        return False
    parent = path.parent
    try:
        if kind == "zip":
            if not zipfile.is_zipfile(path):
                raise FetchError(f"truncated zip (no end-of-central-directory): {path}")
            with zipfile.ZipFile(path) as z:
                z.extractall(parent)
        elif kind == "tar":
            with tarfile.open(path) as t:
                t.extractall(parent, filter="data")
        elif kind in ("gzip", "bzip2", "xz"):
            mode = {"gzip": "r:gz", "bzip2": "r:bz2", "xz": "r:xz"}[kind]
            opener = {"gzip": gzip.open, "bzip2": bz2.open, "xz": lzma.open}[kind]
            try:
                with tarfile.open(path, mode) as t:
                    t.extractall(parent, filter="data")
            except tarfile.ReadError:
                _extract_single_compressed(path, opener)
    except FetchError:
        raise
    except Exception as e:
        raise FetchError(f"extract {kind} {path.name}: {e}") from e
    path.unlink()
    return True


def normalize(target: Path) -> None:
    """Recursively extract archives up to MAX_EXTRACT_DEPTH passes."""
    for _ in range(MAX_EXTRACT_DEPTH):
        changed = False
        for p in sorted(target.rglob("*")):
            if p.is_file() and not p.name.startswith("."):
                if extract_archive(p):
                    changed = True
        if not changed:
            return


# ---------- fetchers ----------


def fetch_github(entry: dict, target: Path) -> dict:
    """Clone into target (which must not exist yet — git creates it)."""
    repo = entry["gh_repo"]
    url = f"https://github.com/{repo}.git"
    ref = entry.get("gh_ref")
    cmd = ["git", "clone", url, str(target)]
    if not ref:
        cmd[2:2] = ["--depth", "1"]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise FetchError(f"git clone failed ({e.returncode}): {repo}")
    if ref:
        try:
            subprocess.run(["git", "-C", str(target), "checkout", ref], check=True)
        except subprocess.CalledProcessError as e:
            raise FetchError(f"git checkout '{ref}' failed: {e}")
    return {"method": "git clone + checkout" if ref else "git clone --depth 1",
            "repo": repo, "ref": ref}


def fetch_hf(entry: dict, target: Path) -> dict:
    """Download an HF dataset via `huggingface-cli` into target (or target/<subdir>)."""
    repo = entry["hf_dataset"]
    subdir = entry.get("hf_subdir", "")
    revision = entry.get("hf_revision")
    local_dir = (target / subdir) if subdir else target
    local_dir.mkdir(parents=True, exist_ok=True)
    cli = shutil.which("hf") or shutil.which("huggingface-cli")
    if not cli:
        raise FetchError(
            "HuggingFace CLI not on PATH. Install: pip install -U 'huggingface_hub[cli]'"
        )
    cmd = [cli, "download", repo, "--repo-type", "dataset",
           "--local-dir", str(local_dir)]
    if revision:
        cmd += ["--revision", revision]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout:
        sys.stdout.write(proc.stdout)
    if proc.stderr:
        sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        blob = (proc.stderr + "\n" + proc.stdout).lower()
        gated_markers = ("access denied", "requires approval", "gated",
                         "401", "403", "authentication", "you are not authorized",
                         "must be authenticated", "request access")
        url = f"https://huggingface.co/datasets/{repo}"
        if any(m in blob for m in gated_markers):
            raise NeedsManual(url, "HuggingFace dataset is gated / auth-required")
        raise FetchError(f"{Path(cli).name} download failed ({proc.returncode}): {repo}")
    return {"method": f"{Path(cli).name} download --repo-type dataset",
            "repo": repo, "subdir": subdir or ".", "revision": revision}


def fetch_http(entry: dict, target: Path) -> dict:
    urls = entry.get("urls") or [entry["url"]]
    target.mkdir(parents=True, exist_ok=True)
    files = []
    bytes_total = 0
    for url in urls:
        final, size = http_download(url, target)
        files.append(final.name)
        bytes_total += size
    return {"method": "http", "urls": urls, "files": files, "downloaded_bytes": bytes_total}


FETCHERS = {"github": fetch_github, "hf": fetch_hf, "http": fetch_http}


# ---------- expects contract ----------


def verify_expects(target: Path, expects: dict | None,
                   entries: list["DirEntry"]) -> str | None:
    """Return None if the contract is satisfied (or absent), else a failure reason string."""
    if not expects:
        return None
    reasons = []
    min_mb = expects.get("min_size_mb")
    if min_mb is not None:
        final_mb = sum(e.size for e in entries) / 1e6
        if final_mb < min_mb:
            reasons.append(f"final_size {final_mb:.1f} MB < expected min {min_mb} MB")
    for rel in expects.get("contains") or []:
        if not (target / rel).exists():
            reasons.append(f"missing expected path: {rel}")
    return "; ".join(reasons) if reasons else None


# ---------- second-stage data source detection + auto-resolve ----------
#
# A github clone may be only the code side of a dataset, with the actual data
# hosted on Zenodo / Figshare / Dataverse / HF / Kaggle / Drive and pulled by
# an in-tree download script. Clone success alone does not mean "ready to use",
# so we scan for these signals and try to fetch them inline before falling back
# to DOWNLOAD_ME.

_HIDDEN_SCRIPT_GLOBS = ("download_*.sh", "download_*.py",
                        "fetch_*.sh", "fetch_*.py",
                        "prepare_data*", "get_data*")

_HIDDEN_URL_RE = re.compile(
    r"https?://[^\s)'\"<>\]]*?"
    r"(?:zenodo\.org|figshare\.com|dataverse\.harvard\.edu"
    r"|huggingface\.co/datasets|kaggle\.com/datasets"
    r"|drive\.google\.com)"
    r"[^\s)'\"<>\]]*",
    re.IGNORECASE,
)

_ZENODO_RECORD_RE = re.compile(
    r"(https?://(?:[\w-]+\.)*zenodo\.org)/(?:api/)?records?/(\d+)", re.IGNORECASE)


def _normalize_data_url(url: str) -> str:
    """Turn a landing-style data-host URL into a direct-fetch endpoint."""
    # bare Zenodo landing (records/NNN with no file path) → files-archive API
    m = re.match(
        r"(https?://(?:[\w-]+\.)*zenodo\.org)/records?/(\d+)/?$", url, re.IGNORECASE)
    if m:
        return f"{m.group(1)}/api/records/{m.group(2)}/files-archive"
    return url


def _dedupe_per_record(urls: list[str]) -> list[str]:
    """Keep at most one URL per Zenodo record, preferring a direct file URL
    (`/files/<name>`) over a files-archive or bare landing form."""
    by_record: dict[str, list[str]] = {}
    passthrough: list[str] = []
    for u in urls:
        m = _ZENODO_RECORD_RE.search(u)
        if m:
            by_record.setdefault(m.group(2), []).append(u)
        else:
            passthrough.append(u)
    chosen = []
    for _rec, candidates in by_record.items():
        direct = [u for u in candidates
                  if "/files/" in u and "/files-archive" not in u]
        chosen.append(direct[0] if direct else candidates[0])
    return sorted(set(chosen + passthrough))


def scan_tree(target: Path) -> tuple[list[str], set[str]]:
    """Return (download_script_paths, raw_data_host_urls) from scripts + README."""
    scripts = sorted({
        str(p.relative_to(target))
        for g in _HIDDEN_SCRIPT_GLOBS
        for p in target.rglob(g)
        if p.is_file()
    })
    urls: set[str] = set()
    for g in (*_HIDDEN_SCRIPT_GLOBS, "README*"):
        for p in target.rglob(g):
            if p.is_file() and p.stat().st_size < 1_000_000:
                try:
                    urls.update(_HIDDEN_URL_RE.findall(p.read_text(errors="ignore")))
                except OSError:
                    continue
    return scripts, urls


def scan_hidden_sources(target: Path) -> list[str]:
    """Human-readable hints when scan_tree finds potential unfetched data."""
    scripts, urls = scan_tree(target)
    hints = []
    if scripts:
        hints.append("in-tree download scripts: " + ", ".join(scripts[:5]))
    if urls:
        hints.append("data-host URLs: " + ", ".join(sorted(urls)[:5]))
    return hints


def extract_hidden_urls(target: Path) -> list[str]:
    """URLs from scan_tree, normalized to direct-fetch form and deduped per record."""
    _, urls = scan_tree(target)
    return _dedupe_per_record([_normalize_data_url(u) for u in urls])


def should_scan_hidden(entry: dict, sources: list[str]) -> bool:
    """Only scan github-only entries without an explicit expects contract:
    with `expects` satisfied the user has already vouched for completeness;
    when other sources are present the user has already extended the registry."""
    return sources == ["github"] and not entry.get("expects")


def auto_resolve_hidden(entry: dict, target: Path,
                        sources: list[str]) -> dict | None:
    """For github-only entries without expects, fetch any data-host URLs
    discovered in the tree. Returns an http-meta dict on any success, None if
    the gate does not apply or nothing was discovered, raises NeedsManual if
    candidates were found but every fetch attempt failed."""
    if not should_scan_hidden(entry, sources):
        return None
    urls = extract_hidden_urls(target)
    if not urls:
        return None
    successes: list[tuple[str, str, int]] = []
    failures: list[tuple[str, str]] = []
    for url in urls:
        try:
            final, size = http_download(url, target)
            successes.append((url, final.name, size))
        except NeedsManual as e:
            failures.append((url, f"needs manual: {e.reason}"))
        except FetchError as e:
            failures.append((url, str(e)))
    if not successes:
        raise NeedsManual(
            "; ".join(u for u, _ in failures),
            f"auto-fetch failed for {len(failures)} discovered URL(s)")
    meta = {
        "method": "auto-resolved http (from in-tree hints)",
        "urls": [u for u, _, _ in successes],
        "files": [f for _, f, _ in successes],
        "downloaded_bytes": sum(s for _, _, s in successes),
    }
    if failures:
        meta["skipped"] = [{"url": u, "error": e} for u, e in failures]
    return meta


# ---------- MANIFEST / DOWNLOAD_ME ----------


@dataclass
class DirEntry:
    name: str
    size: int


def list_entries(target: Path) -> list[DirEntry]:
    out = []
    for p in sorted(target.iterdir()):
        if p.name in (MANIFEST, PENDING_META, _LEGACY_DOWNLOAD_ME) \
                or p.name.endswith(".part"):
            continue
        if p.is_dir():
            size = sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
            out.append(DirEntry(p.name + "/", size))
        else:
            out.append(DirEntry(p.name, p.stat().st_size))
    return out


def render_manifest(slug: str, entry: dict, metas: dict, elapsed: float,
                    downloaded_bytes: int, entries: list[DirEntry],
                    *, phase: str = "fetch",
                    detected_requires: list[str] | None = None) -> str:
    """Render MANIFEST.md. `source` reflects registry intent; `method` reflects
    what actually ran this invocation (derived from metas keys); `elapsed` is
    annotated with what it measures so it never silently lies."""
    final_size = sum(e.size for e in entries)
    method = "+".join(metas.keys()) or "?"
    if phase == "fetch":
        elapsed_line = f"{elapsed:.1f}s"
    elif phase == "promote":
        elapsed_line = (f"{elapsed:.1f}s (promote only; original download time "
                        "not tracked)")
    else:  # remanifest
        elapsed_line = "(remanifest; fetch time unknown)"
    declared = list(entry.get("requires") or [])
    detected = list(detected_requires or [])
    combined = sorted(set(declared) | set(detected))
    parts = [
        f"# {slug}",
        "",
        f"- fetched: {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        f"- source: {source_label(entry)}",
        f"- method: {method}",
        f"- downloaded_size: {downloaded_bytes / 1e6:.1f} MB",
        f"- final_size: {final_size / 1e6:.1f} MB",
        f"- elapsed: {elapsed_line}",
        f"- fetch_meta: {json.dumps(metas, ensure_ascii=False)}",
    ]
    if combined:
        declared_set = set(declared)
        rendered = [r if r in declared_set else f"{r} (auto)" for r in combined]
        parts.append(f"- requires: {', '.join(rendered)}")
    parts += ["", "## entries", ""]
    for e in entries:
        parts.append(f"- {e.name}  ({e.size / 1e6:.1f} MB)")
    if expects := entry.get("expects"):
        parts += ["", "## expects (all satisfied)", "",
                  "```yaml", yaml.safe_dump(expects, sort_keys=True).rstrip(), "```"]
    if caveat := filter_stale_caveat(entry.get("caveat")):
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


def render_pending_section(slug: str, entry: dict, meta: dict) -> str:
    """Render one slug's block in the top-level DOWNLOAD_ME aggregate."""
    reason = meta.get("reason", "")
    attempted = meta.get("attempted") or []
    blocked_url = meta.get("blocked_url")
    hidden_hints = meta.get("hidden_hints") or []
    target = slug_dir(slug)
    lines = [
        f"## {slug}",
        "",
        f"- Reason: {reason}",
    ]
    if blocked_url:
        lines.append(f"- Blocked URL: {blocked_url}")
    lines.append(f"- Sources attempted: {', '.join(attempted) or '(none)'}")
    lines.append(f"- Target directory: {target}")
    if expects := entry.get("expects"):
        lines += [f"- Expects contract: {json.dumps(expects, ensure_ascii=False)}"]
    lines += ["", "### How to fix", ""]

    is_hf = blocked_url and "huggingface.co" in blocked_url
    if hidden_hints:
        lines += [
            "The primary source was fetched, but the tree references a "
            "second-stage data source that was not pulled automatically. "
            "Inspect the hints below and extend the registry:",
            "",
        ]
        lines += [f"- {h}" for h in hidden_hints]
        lines += [
            "",
            "Preferred recovery — point the registry at the real data host "
            "(Zenodo record id, Figshare article, Dataverse DOI, …) and "
            "force-refetch so the bundle becomes fully automatic:",
            "",
            "    ```bash",
            f"    dataset add {slug} --force --gh OWNER/NAME --url REAL_DATA_URL ...",
            f"    dataset fetch {slug} --force",
            "    ```",
            "",
            "If no stable URL exists, run the in-tree script (e.g. "
            "`bash download_*.sh`) and then declare an explicit expects "
            "contract so future fetches verify completeness:",
            "",
            "    ```bash",
            f"    dataset add {slug} --force --gh OWNER/NAME "
            "--expect-min-size-mb 500 --expect-contains data/",
            f"    dataset fetch {slug} --force",
            "    ```",
            "",
        ]
    elif is_hf:
        lines += [
            "HuggingFace gated repo — account-level consent unlocks a CDN fetch "
            "that flows directly to this server, no manual file hop.",
            "",
            f"1. Open {blocked_url} in a browser, click the access button.",
            "2. Create a read token at https://huggingface.co/settings/tokens.",
            "3. On the server:",
            "",
            "```bash",
            "hf auth login                    # paste the token (one-time)",
            f"dataset fetch {slug} --force",
            "```",
            "",
            "If you cannot obtain access, remove the `--hf` source from the "
            "registry so the slug covers only the public parts.",
        ]
    else:
        lines += [
            "Source is bot-blocked / login-gated / private share / landing page "
            "without a direct file link. Two ways to recover:",
            "",
            "1. Agent with browser tools (preferred — no Windows hop): have a "
            "Claude agent with Playwright MCP open the URL, pass any JS "
            "challenge, click Download, and drop the file on the server. The "
            "bytes never leave the datacenter.",
            "",
            "2. Manual browser + scp (if no agent is available):",
            "",
            "```bash",
            f"# after downloading in a browser on your laptop:",
            f"scp <downloaded-file> SERVER:{target}/",
            f"dataset fetch {slug}         # promotes manual drop to MANIFEST",
            "```",
            "",
            "If the data has a stable direct URL (the browser network tab often "
            "exposes one), register it so future fetches are fully automatic:",
            "",
            "```bash",
            f"dataset add {slug} --force --url DIRECT_URL  # keep other sources",
            f"dataset fetch {slug} --force",
            "```",
        ]
    return "\n".join(lines)


def render_top_download_me(pendings: list[dict]) -> str:
    reg = load_registry()
    n = len(pendings)
    head = [
        f"# /home/datasets — {n} item{'s' if n != 1 else ''} pending manual completion",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        "",
        "## Summary",
        "",
    ]
    for m in pendings:
        head.append(f"- `{m['slug']}` — {m.get('reason', '(no reason recorded)')}")
    head += ["", "---", ""]
    sections = [render_pending_section(m["slug"], reg.get(m["slug"], {}), m)
                for m in pendings]
    return "\n".join(head) + "\n" + "\n\n---\n\n".join(sections) + "\n"


def _load_pending(slug_path: Path) -> dict | None:
    p = slug_path / PENDING_META
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def rebuild_top_download_me() -> None:
    """Scan all slug dirs for `.pending.json` and rewrite (or delete) the
    top-level aggregate. Called after any state change."""
    if not ROOT.exists():
        return
    pendings = []
    for d in sorted(ROOT.iterdir()):
        if d.is_dir():
            meta = _load_pending(d)
            if meta:
                meta.setdefault("slug", d.name)
                pendings.append(meta)
    if not pendings:
        TOP_DOWNLOAD_ME.unlink(missing_ok=True)
        return
    TOP_DOWNLOAD_ME.write_text(render_top_download_me(pendings))


def write_pending(slug: str, reason: str, attempted: list[str],
                  blocked_url: str | None = None,
                  hidden_hints: list[str] | None = None) -> None:
    d = slug_dir(slug)
    d.mkdir(parents=True, exist_ok=True)
    meta = {
        "slug": slug,
        "reason": reason,
        "attempted": attempted,
        "blocked_url": blocked_url,
        "hidden_hints": hidden_hints or [],
        "recorded": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (d / PENDING_META).write_text(json.dumps(meta, indent=2, ensure_ascii=False))
    (d / _LEGACY_DOWNLOAD_ME).unlink(missing_ok=True)
    rebuild_top_download_me()


def clear_pending(slug: str) -> None:
    d = slug_dir(slug)
    (d / PENDING_META).unlink(missing_ok=True)
    (d / _LEGACY_DOWNLOAD_ME).unlink(missing_ok=True)
    rebuild_top_download_me()


# ---------- status inspection ----------


def parse_manifest_entries(manifest_text: str) -> list[str]:
    names = []
    in_entries = False
    for line in manifest_text.splitlines():
        if line.strip() == "## entries":
            in_entries = True
            continue
        if in_entries:
            if line.startswith("## "):
                break
            m = re.match(r"^- (.+?)\s\s", line) or re.match(r"^- (.+)$", line)
            if m:
                names.append(m.group(1).strip())
    return names


def slug_status(slug: str) -> tuple[str, str]:
    """Return (code, note). code in {complete, pending, missing}."""
    d = slug_dir(slug)
    if not d.exists():
        return ("missing", "")
    if (d / PENDING_META).exists():
        return ("pending", "manual completion required")
    if (d / _LEGACY_DOWNLOAD_ME).exists():  # pre-migration marker
        return ("pending", "legacy DOWNLOAD_ME.md — rerun fetch to migrate")
    manifest = d / MANIFEST
    if not manifest.exists():
        orphans = [p for p in d.iterdir() if p.name != MANIFEST]
        if orphans:
            return ("pending", "orphan artifacts — rerun fetch")
        return ("missing", "")
    for name in parse_manifest_entries(manifest.read_text()):
        p = d / name.rstrip("/")
        if not p.exists():
            return ("pending", f"missing entry: {name}")
    return ("complete", "")


# ---------- commands ----------


def cmd_fetch(args) -> int:
    reg = load_registry()
    entry = reg.get(args.slug)
    if entry is None:
        print(f"error: '{args.slug}' not in registry (use 'dataset add' first)", file=sys.stderr)
        return EXIT_USAGE
    sources = active_sources(entry)
    if not sources:
        print(f"error: '{args.slug}' has no configured sources (add --gh / --hf / --url)",
              file=sys.stderr)
        return EXIT_USAGE

    target = slug_dir(args.slug)
    lock = acquire_lock(args.slug)
    try:
        status, _ = slug_status(args.slug)
        pending_path = target / PENDING_META

        if status == "complete" and not args.force:
            print(f"already cached: {target} (--force to refetch)")
            return EXIT_OK

        if pending_path.exists() and not args.force:
            manual_files = [
                p for p in target.iterdir()
                if p.name not in (MANIFEST, PENDING_META, _LEGACY_DOWNLOAD_ME)
                and not p.name.endswith(".part")
            ]
            if manual_files:
                return _promote_manual(args.slug, entry, target)
            print(f"pending: {TOP_DOWNLOAD_ME} (see section for {args.slug})",
                  file=sys.stderr)
            return EXIT_PENDING

        if target.exists():
            shutil.rmtree(target)

        metas: dict[str, dict] = {}
        t0 = time.time()
        try:
            for src in sources:
                metas[src] = FETCHERS[src](entry, target)
            normalize(target)
            auto_meta = auto_resolve_hidden(entry, target, sources)
            if auto_meta:
                metas["auto"] = auto_meta
                normalize(target)
        except NeedsManual as e:
            shutil.rmtree(target, ignore_errors=True)
            write_pending(args.slug, e.reason, sources, blocked_url=e.url)
            print(f"pending: {e.reason} (see {TOP_DOWNLOAD_ME})", file=sys.stderr)
            return EXIT_PENDING
        except FetchError as e:
            shutil.rmtree(target, ignore_errors=True)
            print(f"fetch failed: {e}", file=sys.stderr)
            print("registry entry:\n"
                  + yaml.safe_dump({args.slug: entry}, allow_unicode=True, sort_keys=True),
                  file=sys.stderr)
            return EXIT_ERROR

        entries = list_entries(target)
        expects_fail = verify_expects(target, entry.get("expects"), entries)
        if expects_fail:
            shutil.rmtree(target, ignore_errors=True)
            write_pending(args.slug,
                          f"expects contract not met — {expects_fail}",
                          sources)
            print(f"pending: expects contract not met — {expects_fail}", file=sys.stderr)
            return EXIT_PENDING

        if "auto" not in metas and should_scan_hidden(entry, sources):
            hidden = scan_hidden_sources(target)
            if hidden:
                write_pending(args.slug,
                              "repo references a second-stage data source that "
                              "was not fetched",
                              sources, hidden_hints=hidden)
                print(f"pending: second-stage data source detected "
                      f"({'; '.join(hidden)})", file=sys.stderr)
                return EXIT_PENDING

        elapsed = time.time() - t0
        downloaded = sum(m.get("downloaded_bytes", 0) for m in metas.values()) \
            or sum(e.size for e in entries)
        (target / MANIFEST).write_text(
            render_manifest(args.slug, entry, metas, elapsed, downloaded, entries,
                            phase="fetch",
                            detected_requires=detect_requires(target)))
        clear_pending(args.slug)
        final_size = sum(e.size for e in entries)
        print(f"ok: {target} ({final_size / 1e6:.1f} MB final, "
              f"{downloaded / 1e6:.1f} MB downloaded, {elapsed:.1f}s, "
              f"sources={'+'.join(sources)})")
        return EXIT_OK
    finally:
        lock.close()


def _promote_manual(slug: str, entry: dict, target: Path) -> int:
    t0 = time.time()
    sources = active_sources(entry)
    auto_meta: dict | None = None
    try:
        normalize(target)
        auto_meta = auto_resolve_hidden(entry, target, sources)
        if auto_meta:
            normalize(target)
    except NeedsManual as e:
        write_pending(slug, e.reason, sources + ["manual"], blocked_url=e.url)
        print(f"pending: {e.reason}", file=sys.stderr)
        return EXIT_PENDING
    except FetchError as e:
        print(f"normalize failed: {e}", file=sys.stderr)
        return EXIT_ERROR
    entries = list_entries(target)
    expects_fail = verify_expects(target, entry.get("expects"), entries)
    if expects_fail:
        write_pending(slug,
                      f"manual files present but contract still not met — {expects_fail}",
                      sources + ["manual"])
        print(f"pending: expects contract not met — {expects_fail}", file=sys.stderr)
        return EXIT_PENDING
    if not auto_meta and should_scan_hidden(entry, sources):
        hidden = scan_hidden_sources(target)
        if hidden:
            write_pending(slug,
                          "repo still references a second-stage data source; "
                          "extend the registry or declare an expects contract",
                          sources + ["manual"], hidden_hints=hidden)
            print(f"pending: second-stage data source still present "
                  f"({'; '.join(hidden)})", file=sys.stderr)
            return EXIT_PENDING
    elapsed = time.time() - t0
    total = sum(e.size for e in entries)
    metas: dict = {"manual": {"method": "manual", "files": [e.name for e in entries]}}
    if auto_meta:
        metas["auto"] = auto_meta
    (target / MANIFEST).write_text(
        render_manifest(slug, entry, metas, elapsed, total, entries,
                        phase="promote",
                        detected_requires=detect_requires(target)))
    clear_pending(slug)
    print(f"ok (promoted from manual download): {target} "
          f"({total / 1e6:.1f} MB, {elapsed:.1f}s normalize)")
    return EXIT_OK


def cmd_list(args) -> int:
    reg = load_registry()
    if not reg:
        print("(empty)")
        return EXIT_OK
    rebuild_top_download_me()
    marks = {"complete": "✓", "pending": "⋯", "missing": " "}
    pending = 0
    for slug in sorted(reg):
        status, note = slug_status(slug)
        src = source_label(reg[slug])
        line = f"{marks[status]}  {slug:<32s}  {src}"
        if note:
            line += f"  ({note})"
        print(line)
        if status == "pending":
            pending += 1
    if pending:
        print(f"\n{pending} item(s) pending — see {TOP_DOWNLOAD_ME}")
    return EXIT_OK


def cmd_add(args) -> int:
    if not (args.gh or args.hf or args.url):
        print("error: specify at least one of --gh / --hf / --url", file=sys.stderr)
        return EXIT_USAGE

    lock = acquire_lock("registry")
    try:
        reg = load_registry()
        if args.slug in reg and not args.force:
            print(f"error: '{args.slug}' exists (--force to overwrite)", file=sys.stderr)
            return EXIT_USAGE
        entry: dict = {}
        if args.gh:
            entry["gh_repo"] = args.gh
            if args.gh_ref:
                entry["gh_ref"] = args.gh_ref
        if args.hf:
            entry["hf_dataset"] = args.hf
            if args.hf_subdir:
                entry["hf_subdir"] = args.hf_subdir
            if args.hf_revision:
                entry["hf_revision"] = args.hf_revision
        if args.url:
            if len(args.url) == 1:
                entry["url"] = args.url[0]
            else:
                entry["urls"] = args.url
        if args.caveat:
            entry["caveat"] = args.caveat
        if args.requires:
            entry["requires"] = list(args.requires)
        expects = {}
        if args.expect_min_size_mb is not None:
            expects["min_size_mb"] = args.expect_min_size_mb
        if args.expect_contains:
            expects["contains"] = list(args.expect_contains)
        if expects:
            entry["expects"] = expects
        # keep a derived `source` field for readability of registry.yaml
        entry["source"] = source_label(entry)
        reg[args.slug] = entry
        save_registry(reg)
        print(f"added: {args.slug} [{entry['source']}]")
        return EXIT_OK
    finally:
        lock.close()


def cmd_remanifest(args) -> int:
    """Rewrite MANIFEST.md from what is already on disk. No network, no fetch.
    Use after editing the registry or upgrading the tool so the new schema
    (method/elapsed/requires) applies without a multi-GB re-download."""
    reg = load_registry()
    entry = reg.get(args.slug)
    if entry is None:
        print(f"error: '{args.slug}' not in registry", file=sys.stderr)
        return EXIT_USAGE
    target = slug_dir(args.slug)
    if not target.exists():
        print(f"error: {target} does not exist — nothing to remanifest",
              file=sys.stderr)
        return EXIT_ERROR
    lock = acquire_lock(args.slug)
    try:
        entries = list_entries(target)
        if not entries:
            print(f"error: {target} is empty", file=sys.stderr)
            return EXIT_ERROR
        total = sum(e.size for e in entries)
        metas: dict = {"manual": {"method": "manual",
                                  "files": [e.name for e in entries]}}
        (target / MANIFEST).write_text(
            render_manifest(args.slug, entry, metas, 0.0, total, entries,
                            phase="remanifest",
                            detected_requires=detect_requires(target)))
        clear_pending(args.slug)
        print(f"ok (remanifested): {target} ({total / 1e6:.1f} MB)")
        return EXIT_OK
    finally:
        lock.close()


def cmd_manifest(args) -> int:
    d = slug_dir(args.slug)
    manifest = d / MANIFEST
    if manifest.exists():
        print(manifest.read_text(), end="")
        return EXIT_OK
    if (d / PENDING_META).exists() or (d / _LEGACY_DOWNLOAD_ME).exists():
        if TOP_DOWNLOAD_ME.exists():
            print(f"# {args.slug} is pending — full instructions in {TOP_DOWNLOAD_ME}\n")
            print(TOP_DOWNLOAD_ME.read_text(), end="")
        else:
            print(f"pending but aggregate missing; run `dataset list` to rebuild",
                  file=sys.stderr)
        return EXIT_OK
    print(f"not fetched: {args.slug}", file=sys.stderr)
    return EXIT_ERROR


def main() -> int:
    p = argparse.ArgumentParser(prog="dataset", description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    f = sub.add_parser("fetch", help="fetch a registered dataset (or promote a manual download)")
    f.add_argument("slug")
    f.add_argument("--force", action="store_true", help="refetch even if already complete/pending")
    f.set_defaults(func=cmd_fetch)

    l = sub.add_parser("list", help="list registered datasets with status")
    l.set_defaults(func=cmd_list)

    a = sub.add_parser("add", help="register a new dataset (combinable sources)")
    a.add_argument("slug")
    a.add_argument("--gh", metavar="OWNER/NAME", help="GitHub repo")
    a.add_argument("--gh-ref", metavar="REF", help="git branch / tag / commit")
    a.add_argument("--hf", metavar="OWNER/NAME", help="HuggingFace dataset id")
    a.add_argument("--hf-subdir", metavar="DIR", help="place HF files under this subdir of the slug")
    a.add_argument("--hf-revision", metavar="REV", help="HF revision / tag / commit")
    a.add_argument("--url", action="append", metavar="URL", help="direct URL (repeatable)")
    a.add_argument("--caveat", metavar="TEXT",
                   help="post-fetch note about the data itself (license, citation, "
                        "loading quirks). Fetch-time obstacles should go to "
                        "DOWNLOAD_ME via the pending flow, not here — this field is "
                        "auto-pruned of sentences that become false after fetch.")
    a.add_argument("--requires", action="append", metavar="PKG",
                   help="Python package needed to load the files (repeatable). "
                        "Also auto-detected from serialized-object opcodes and "
                        "file extensions, but declaring is authoritative.")
    a.add_argument("--expect-min-size-mb", type=float, metavar="MB",
                   help="contract: final_size must be at least this many MB")
    a.add_argument("--expect-contains", action="append", metavar="PATH",
                   help="contract: this path (relative to slug dir) must exist (repeatable)")
    a.add_argument("--force", action="store_true", help="overwrite existing entry")
    a.set_defaults(func=cmd_add)

    m = sub.add_parser("manifest", help="print MANIFEST.md or DOWNLOAD_ME.md for a slug")
    m.add_argument("slug")
    m.set_defaults(func=cmd_manifest)

    rm = sub.add_parser("remanifest",
                        help="regenerate MANIFEST.md from what is already on disk "
                             "(no refetch) — useful after registry edits or tool upgrades")
    rm.add_argument("slug")
    rm.set_defaults(func=cmd_remanifest)

    args = p.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
