#!/bin/bash
# Create the local Python env used by the installer (Python 3.10+).
set -euo pipefail
PKG="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PYTHON:-python3}"
"$PYTHON" -c 'import sys; assert sys.version_info >= (3, 10), "Python 3.10+ required"'
"$PYTHON" -m venv "$PKG/.venv"
"$PKG/.venv/bin/pip" install -q --upgrade pip
"$PKG/.venv/bin/pip" install -q -r "$PKG/installer-requirements.txt"
command -v databricks >/dev/null || { echo "Databricks CLI not found: https://docs.databricks.com/dev-tools/cli/install.html" >&2; exit 1; }
echo "OK: $("$PKG/.venv/bin/python" -c 'import databricks.sdk as s; print("databricks-sdk", s.version.__version__)'), $(databricks --version)"
