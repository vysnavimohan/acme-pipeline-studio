#!/bin/bash
# Install the app. Usage: bin/install_app.sh [all|skills|genie-job|app|seed|status]   (default: all)
source "$(dirname "$0")/_common.sh"
exec "$PY" "$PKG/platform/install_app.py" "${1:-all}"
