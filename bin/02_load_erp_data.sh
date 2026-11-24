#!/bin/bash
# Generate the synthetic ERP extracts (104 weeks, 10 feeds) and upload them to cfo_workbench.raw_erp.
source "$(dirname "$0")/_common.sh"
exec "$PY" "$PKG/datasets/py/setup_datasets.py" erp-data
