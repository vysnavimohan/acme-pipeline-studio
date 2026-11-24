#!/bin/bash
# Create + run (full refresh) the serverless SDP pipeline: raw_erp -> bronze -> silver -> gold in cfo_workbench.
source "$(dirname "$0")/_common.sh"
exec "$PY" "$PKG/datasets/py/setup_datasets.py" erp-pipeline
