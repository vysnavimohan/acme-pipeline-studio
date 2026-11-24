#!/bin/bash
# Check row counts and the planted demo numbers (Fasteners -140 bps vs plan, DSO ~47 days, 49,653 shipments).
source "$(dirname "$0")/_common.sh"
exec "$PY" "$PKG/datasets/py/setup_datasets.py" verify
