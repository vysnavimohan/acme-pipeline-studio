"""Landing-zone access — the Unity Catalog Volume that simulates the Databricks
ADLS2 landing container.

In the Databricks DSD, vendor files (ERP extracts, CSV/DAT) are
pulled via MFT into an ADLS2 landing zone, then Auto Loader ingests them to
Bronze. For the PoC we simulate that container with a **UC managed Volume**:
new datasets arrive as files in per-dataset subdirectories, e.g.

    /Volumes/<catalog>/<schema>/landing/sales_invoice_lines/*.csv

The agent's Auto Loader bronze ingestion points `cloudFiles` at the dataset
directory. This module lists datasets/files and profiles them (delegating to
`file_profiler`), and can ensure the volume exists (used by the one-line deploy).
"""

from __future__ import annotations

import os
import posixpath
from typing import Any, Optional

from common import file_profiler

# Volume identity: catalog.schema.volume. Overridable per-workspace.
_DEFAULT_VOLUME = "vmohan_demo_catalog.cfo_workbench.raw_erp"


def volume_fqn() -> str:
    return os.environ.get("OPS_LANDING_VOLUME", _DEFAULT_VOLUME).strip()


def volume_parts() -> tuple[str, str, str]:
    cat, sch, vol = volume_fqn().split(".", 2)
    return cat, sch, vol


def volume_root() -> str:
    cat, sch, vol = volume_parts()
    return f"/Volumes/{cat}/{sch}/{vol}"


def dataset_dir(name: str) -> str:
    return posixpath.join(volume_root(), name)


def _client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


def _list_dir(path: str) -> list[Any]:
    """List a directory, swallowing errors (returns [] on failure). Use for
    per-subdir file listing where a missing/empty dir is fine."""
    try:
        return list(_client().files.list_directory_contents(path))
    except Exception:
        return []


def _list_dir_strict(path: str) -> list[Any]:
    """List a directory, RAISING on error — so a bad/ungranted root surfaces a
    clear message to the caller (e.g. the Add-source / Refresh API)."""
    return list(_client().files.list_directory_contents(path))


# --- generic helpers: operate on ANY UC Volume path (not just the env volume) ---

def glob_for_dir(dir_path: str, latest_file: Optional[str] = None) -> str:
    """The cloudFiles glob Auto Loader watches for a dataset directory — all
    files of the latest file's extension (or '*' when unknown)."""
    ext = posixpath.splitext(latest_file)[1] if latest_file else ""
    return posixpath.join(dir_path.rstrip("/"), f"*{ext}" if ext else "*")


def list_files(dir_path: str) -> list[dict[str, Any]]:
    """Files (most-recent first) in a directory."""
    files = [f for f in _list_dir(dir_path) if not getattr(f, "is_directory", False)]
    files_sorted = sorted(files, key=lambda f: getattr(f, "last_modified", 0) or 0, reverse=True)
    return [{"name": posixpath.basename(f.path), "path": f.path,
             "size": getattr(f, "file_size", 0)} for f in files_sorted]


def list_subdirs(root: str) -> list[dict[str, Any]]:
    """Each immediate subdirectory of `root` is a 'dataset'. Returns name +
    file count + total bytes + most-recent file. RAISES if `root` can't be
    listed (so the caller can surface a grant error)."""
    out: list[dict[str, Any]] = []
    for entry in _list_dir_strict(root):
        if not getattr(entry, "is_directory", False):
            continue
        name = posixpath.basename(entry.path.rstrip("/"))
        files = list_files(entry.path)
        out.append({
            "name": name,
            "path": entry.path.rstrip("/"),
            "file_count": len(files),
            "total_bytes": sum(f.get("size", 0) or 0 for f in files),
            "latest_file": (files[0]["path"] if files else None),
            "files": files[:25],
        })
    return sorted(out, key=lambda d: d["name"])


def profile_latest_file(dir_path: str, max_bytes: int = 256 * 1024) -> dict[str, Any]:
    """Profile the most-recent file in any directory. Adds source_path +
    ingest_glob to the file_profiler output."""
    files = list_files(dir_path)
    if not files:
        return {"error": "no files in directory", "dir_path": dir_path}
    latest = files[0]["path"]
    prof = file_profiler.profile_volume_file(latest, max_bytes=max_bytes)
    prof["source_path"] = latest
    prof["ingest_glob"] = glob_for_dir(dir_path, latest)
    return prof


def validate_volume_path(path: str) -> Optional[str]:
    """Return None if `path` is a listable UC Volume path, else a clear error
    string (used by the Add-source API before persisting)."""
    p = (path or "").strip()
    if not p.startswith("/Volumes/"):
        return "Path must be a Unity Catalog Volume path starting with /Volumes/…"
    try:
        _list_dir_strict(p)
        return None
    except Exception as e:  # noqa: BLE001
        return (f"Cannot list '{p}': {str(e)[:200]}. Ensure the app service "
                "principal has READ (USE/READ VOLUME) on this volume.")


# --- the default landing volume (back-compat: delegates to the generic helpers) ---

def list_datasets() -> list[dict[str, Any]]:
    """Datasets in the default landing volume (OPS_LANDING_VOLUME)."""
    return list_subdirs(volume_root())


def profile_dataset(name: str, max_bytes: int = 256 * 1024) -> dict[str, Any]:
    """Profile the most-recent file in a default-landing dataset directory."""
    prof = profile_latest_file(dataset_dir(name), max_bytes=max_bytes)
    prof["dataset"] = name
    if "error" in prof:
        prof["error"] = "no files in dataset directory"
    return prof


# --- provisioning (used by the one-line deploy) ---------------------------

def ensure_volume(log=None) -> str:
    """Create the landing schema + managed volume if they don't exist.
    Returns the volume root path. Idempotent."""
    cat, sch, vol = volume_parts()
    w = _client()
    def _log(m):
        if log:
            log(m)
    try:
        w.schemas.get(f"{cat}.{sch}")
    except Exception:
        w.schemas.create(name=sch, catalog_name=cat,
                         comment="Landing zone (simulates ADLS2): vendor files per dataset")
        _log(f"created schema {cat}.{sch}")
    try:
        w.volumes.read(f"{cat}.{sch}.{vol}")
    except Exception:
        from databricks.sdk.service.catalog import VolumeType
        w.volumes.create(catalog_name=cat, schema_name=sch, name=vol,
                         volume_type=VolumeType.MANAGED,
                         comment="Landing zone volume — vendor extracts per dataset")
        _log(f"created volume {cat}.{sch}.{vol}")
    return volume_root()


def upload_file(dataset: str, filename: str, content: bytes) -> str:
    """Upload bytes into <volume>/<dataset>/<filename>. Returns the file path."""
    return upload_to_dir(dataset_dir(dataset), filename, content)


def upload_to_dir(dir_path: str, filename: str, content: bytes) -> str:
    """Upload bytes into <dir_path>/<filename> (any UC Volume dir). Returns the
    file path. Creates the directory if missing."""
    w = _client()
    try:
        w.files.create_directory(dir_path)
    except Exception:
        pass
    dest = posixpath.join(dir_path.rstrip("/"), filename)
    import io
    w.files.upload(dest, io.BytesIO(content), overwrite=True)
    return dest
