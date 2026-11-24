#!/bin/bash
# Create and fill the shared DQ rules repo (cfo_workbench.dq_rules_repo, 10 rules).
source "$(dirname "$0")/_common.sh"
exec "$PY" "$PKG/datasets/py/setup_datasets.py" dq-rules
