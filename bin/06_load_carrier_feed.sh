#!/bin/bash
# Generate the raw carrier-shipments CSVs (delivery-times demo) into studio_control.raw_landing.
source "$(dirname "$0")/_common.sh"
exec "$PY" "$PKG/datasets/py/setup_datasets.py" carrier-feed
