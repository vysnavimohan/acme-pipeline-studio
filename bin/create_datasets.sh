#!/bin/bash
# Build every demo dataset end to end (steps 01-07). Safe to re-run. Takes ~15 min (mostly the pipeline).
source "$(dirname "$0")/_common.sh"
exec "$PY" "$PKG/datasets/py/setup_datasets.py" all
