#!/usr/bin/env bash
# One-time helper on the VPS: clone MPInformer from GitHub into a target directory.
# Run AFTER deploy key or PC SSH auth to github.com is configured for this user.
#
# Usage:
#   bash scripts/vps-git-setup.sh git@github.com:OWNER/MPInformer.git
#   TARGET=/opt/MPInformer bash scripts/vps-git-setup.sh git@github.com:OWNER/MPInformer.git
set -euo pipefail
REPO_URL="${1:?usage: vps-git-setup.sh <git_clone_url>}"
TARGET="${TARGET:-/opt/MPInformer}"

if [[ -e "$TARGET" ]] && [[ -n "$(ls -A "$TARGET" 2>/dev/null)" ]]; then
  echo "[ERROR] Target is not empty: $TARGET"
  echo "[INFO] Remove or pick another TARGET= path. Aborting."
  exit 1
fi

parent="$(dirname "$TARGET")"
sudo mkdir -p "$parent"
sudo git clone "$REPO_URL" "$TARGET"
if [[ "$(id -u)" -ne 0 ]]; then
  sudo chown -R "$(whoami):$(whoami)" "$TARGET"
fi

cd "$TARGET"
git checkout main 2>/dev/null || git checkout -b main "origin/main" 2>/dev/null || true

echo "[OK] Clone finished: $TARGET"
echo "[NEXT] cd $TARGET && python3 -m venv venv && . venv/bin/activate && pip install -r requirements.txt"
echo "[NEXT] cp .env.example .env && nano .env"
echo "[NEXT] See DEPLOY_VPS.md section 7 (systemd mpinformer)"
