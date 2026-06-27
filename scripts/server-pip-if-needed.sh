#!/usr/bin/env bash
# Run on VPS during deploy: pip install only when requirements.txt hash changed.
set -euo pipefail
ROOT="${1:-/opt/MPInformer}"
cd "$ROOT"
CUR_HASH="$(sha256sum requirements.txt | awk '{print $1}')"
OLD_HASH="$(head -c 64 .deploy-requirements.sha256 2>/dev/null | tr -d '\r\n' || true)"
if test -x venv/bin/python && test "$CUR_HASH" = "$OLD_HASH"; then
  echo "[SKIP] requirements unchanged, pip skipped"
  exit 0
fi
python3 -m venv venv
venv/bin/python -m pip install -r requirements.txt
echo "$CUR_HASH" > .deploy-requirements.sha256
echo "[OK] pip install executed"
