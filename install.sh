#!/bin/bash
# One-shot install: Python env -> datasets -> skills + Genie Code job + app + seed.
set -euo pipefail
PKG="$(cd "$(dirname "$0")" && pwd)"
[ -f "$PKG/config.env" ] || { echo "First: cp config.env.example config.env  and fill in PROFILE, CATALOG, WAREHOUSE_ID" >&2; exit 1; }
[ -x "$PKG/.venv/bin/python" ] || "$PKG/bin/00_setup_python.sh"
"$PKG/bin/create_datasets.sh"
"$PKG/bin/install_app.sh" all
