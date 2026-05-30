#!/usr/bin/env bash
# Update to latest version — same as re-running install.sh
set -euo pipefail
exec bash "$(dirname "$0")/install.sh"
