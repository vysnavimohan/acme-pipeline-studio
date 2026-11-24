#!/bin/bash
# Create the catalog schemas (cfo_workbench, studio_control/dev/stage/prod) and landing volumes.
source "$(dirname "$0")/_common.sh"
exec "$PY" "$PKG/datasets/py/setup_datasets.py" schemas
