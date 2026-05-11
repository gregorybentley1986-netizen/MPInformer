#!/usr/bin/env bash
# After git pull on the server: sanity checks + confirm HEAD matches origin/branch.
# Usage: server-pull-verify.sh /path/to/repo [branch] [systemd_unit]
#   branch:        default main
#   systemd_unit:  default mpinformer, or set VERIFY_SYSTEMD_UNIT on the server
set -euo pipefail
REMOTE_PATH="${1:?usage: server-pull-verify.sh /path/to/repo [branch] [systemd_unit]}"
BRANCH="${2:-main}"
UNIT="${3:-${VERIFY_SYSTEMD_UNIT:-mpinformer}}"
cd "$REMOTE_PATH"

echo "==== VERIFY repo ===="
pwd
echo "origin URL:"; git remote get-url origin || true
echo "branch (git):"; git branch --show-current 2>/dev/null || git rev-parse --abbrev-ref HEAD
echo -n "HEAD now: "; git rev-parse HEAD
git log -1 --oneline

echo "==== VERIFY systemd ${UNIT} ===="
if systemctl show "${UNIT}" -p FragmentPath -p WorkingDirectory -p ExecStart --no-pager 2>/dev/null; then
  :
else
  echo "[WARN] systemctl show ${UNIT} unavailable (no permission or wrong unit)."
fi

echo "==== VERIFY project tree ===="
if [[ -f templates/site/printer_card.html ]]; then
  if ! grep -q 'pc-print-action-btn' templates/site/printer_card.html; then
    echo "[ERROR] pc-print-action-btn missing in printer_card.html (PrintFarm tree looks wrong)."
    exit 1
  fi
  echo "[OK] PrintFarm marker in templates/site/printer_card.html."
elif [[ -f app/main.py ]]; then
  if ! grep -q "/health" app/main.py; then
    echo "[ERROR] app/main.py missing /health route (unexpected MPInformer tree)."
    exit 1
  fi
  echo "[OK] MPInformer app/main.py with /health."
else
  echo "[ERROR] Unrecognized repo: no app/main.py (MPInformer) and no templates/site/printer_card.html (PrintFarm)."
  exit 1
fi

echo "==== VERIFY matches origin/${BRANCH} ===="
git fetch "origin" "${BRANCH}" >/dev/null 2>&1 || true
ORIG_REF="origin/${BRANCH}"
if ! git rev-parse --verify "${ORIG_REF}" >/dev/null 2>&1; then
  echo "[ERROR] ref ${ORIG_REF} missing after fetch — check branch name on GitHub."
  exit 1
fi
if [[ "$(git rev-parse HEAD)" != "$(git rev-parse "${ORIG_REF}")" ]]; then
  echo "[WARN] HEAD != ${ORIG_REF}. Run: git checkout ${BRANCH} && git reset --hard ${ORIG_REF}"
  exit 1
fi

echo "[OK] HEAD equals ${ORIG_REF}."
