#!/bin/bash
# Create the governed metric views the app reuses (revenue/margin, working capital, inventory ageing, draft).
source "$(dirname "$0")/_common.sh"
exec "$PY" "$PKG/datasets/py/setup_datasets.py" metric-views
