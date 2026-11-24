#!/bin/bash
# Shared by every bin/ script: resolve the package root and the Python venv.
set -euo pipefail
PKG="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$PKG/.venv/bin/python"
if [ ! -f "$PKG/config.env" ]; then
  echo "config.env not found. Run: cp $PKG/config.env.example $PKG/config.env  (then edit it)" >&2; exit 1
fi
if [ ! -x "$PY" ]; then
  echo "Python env missing. Run: $PKG/bin/00_setup_python.sh" >&2; exit 1
fi
