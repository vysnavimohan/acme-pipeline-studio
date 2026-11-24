"""Landing Zone / Datasets — onboarding endpoints.

The landing zone is a Unity Catalog Volume that simulates the Databricks ADLS2
container. Vendor extracts (CSV/DAT) arrive as files in per-dataset
subdirectories. This API lets an analyst:

  - list datasets currently in the landing zone (GET /api/datasets)
  - profile a dataset's latest file (GET /api/datasets/{name}/profile)
  - upload/onboard a new file into a dataset dir (POST /api/datasets/upload)

A profiled dataset becomes a selectable file/Auto-Loader source for a request.
"""

from __future__ import annotations

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from common import landing_zone

router = APIRouter()


@router.get("")
def list_all(profile: bool = False):
    """List landing-zone datasets. With ?profile=true, also attach each
    dataset's profile (delimiter/columns/etc.) — a bit slower (reads file heads)."""
    try:
        datasets = landing_zone.list_datasets()
    except Exception as e:  # noqa: BLE001
        return {"volume": landing_zone.volume_fqn(), "datasets": [], "error": str(e)[:300]}
    if profile:
        for d in datasets:
            try:
                d["profile"] = landing_zone.profile_dataset(d["name"])
            except Exception as e:  # noqa: BLE001
                d["profile"] = {"error": str(e)[:200]}
    return {"volume": landing_zone.volume_fqn(),
            "volume_root": landing_zone.volume_root(), "datasets": datasets}


@router.get("/{name}/profile")
def profile(name: str):
    try:
        return landing_zone.profile_dataset(name)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)[:300])


@router.post("/upload")
async def upload(file: UploadFile = File(...), dataset: str = Form(...)):
    """Upload a file into <landing>/<dataset>/ and return its fresh profile.
    The dataset name is the landing directory (created if missing)."""
    name = (dataset or "").strip().strip("/")
    if not name:
        raise HTTPException(status_code=400, detail="dataset name required")
    content = await file.read()
    try:
        path = landing_zone.upload_file(name, file.filename, content)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"upload failed: {str(e)[:300]}")
    prof = landing_zone.profile_dataset(name)
    return {"ok": True, "path": path, "dataset": name, "profile": prof}
