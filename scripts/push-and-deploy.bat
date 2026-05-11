@echo off
rem Messages are ASCII-only so CMD (CP866/1252) does not garble UTF-8 Cyrillic.
rem Usage:
rem   push-and-deploy.bat [mode] [ssh_target]
rem   mode:
rem     (empty)   GitHub push + server git sync + pip + restart + wait /health
rem     rollback
rem     localsync  No GitHub: pipe git archive HEAD to server via SSH
rem   ssh_target examples:
rem     root@203.0.113.10
rem     ubuntu@mpi.example.com
rem   Or set MPINFORMER_SSH_TARGET (or SSH_TARGET) instead of the 2nd argument.
rem
rem VPS defaults (MPInformer): /opt/MPInformer, mpinformer, health http://127.0.0.1:8000/health
rem Override: set DEPLOY_HEALTH_URL=...   match systemd uvicorn --port
rem Skip pip on server: set DEPLOY_SKIP_PIP=1
rem Close window without pause: set DEPLOY_NO_PAUSE=1
rem LAN PrintFarm override example:
rem   set "DEPLOY_REMOTE_PATH=/home/esox/PrintFarm"
rem   set "DEPLOY_SERVICE=printfarm"
rem   set "DEPLOY_HEALTH_URL=http://127.0.0.1:8002/health"
rem   push-and-deploy.bat  esox@192.168.0.30
setlocal EnableExtensions EnableDelayedExpansion

set "GIT_BRANCH=main"
set "REMOTE_PATH=/opt/MPInformer"
set "SERVICE=mpinformer"
set "HEALTH_URL=http://127.0.0.1:8000/health"
if defined DEPLOY_REMOTE_PATH set "REMOTE_PATH=!DEPLOY_REMOTE_PATH!"
if defined DEPLOY_SERVICE set "SERVICE=!DEPLOY_SERVICE!"
if defined DEPLOY_HEALTH_URL set "HEALTH_URL=!DEPLOY_HEALTH_URL!"

set "SERVER="
if not "%~2"=="" set "SERVER=%~2"
if "!SERVER!"=="" if defined MPINFORMER_SSH_TARGET set "SERVER=!MPINFORMER_SSH_TARGET!"
if "!SERVER!"=="" if defined SSH_TARGET set "SERVER=!SSH_TARGET!"
if "!SERVER!"=="" if defined PRINTFARM_SSH_TARGET set "SERVER=!PRINTFARM_SSH_TARGET!"
if "!SERVER!"=="" (
  echo [ERROR] SSH target is empty. Set MPINFORMER_SSH_TARGET=root@your-vps
  echo [ERROR] or pass: scripts\push-and-deploy.bat  root@your-vps
  exit /b 1
)

set "RESTART_TIMEOUT_SEC=90"
set "SSH_OPTS=-o ConnectTimeout=15 -o ServerAliveInterval=12 -o ServerAliveCountMax=4"
for /f %%u in ('git remote get-url origin 2^>nul') do set "ORIGIN_URL=%%u"

if /I "%~1"=="rollback" goto rollback

if /I "%~1"=="localsync" (
  call :localsync
  set "RC=!ERRORLEVEL!"
  goto script_finish
)

call :main
set "RC=!ERRORLEVEL!"

:script_finish
echo.
if "!RC!"=="0" (
  echo [DONE] Finished OK.
) else (
  echo [FAILED] Finished with error code !RC!.
)
if defined DEPLOY_NO_PAUSE exit /b !RC!
pause
exit /b !RC!

:rollback
echo ==============================================================
echo Deploy: SERVER ROLLBACK
echo SERVER=!SERVER!
echo REMOTE_PATH=!REMOTE_PATH!
echo SERVICE=!SERVICE!
echo ==============================================================
echo [WARN] This will reset server code to previous commit: HEAD~1
set /p CONFIRM=Type YES to continue rollback: 
if /I not "%CONFIRM%"=="YES" (
  echo [INFO] Rollback cancelled.
  pause
  exit /b 0
)

echo [1/3] Reset server repo to HEAD~1 ...
ssh %SSH_OPTS% !SERVER! "cd !REMOTE_PATH! && git rev-parse --short HEAD && git reset --hard HEAD~1 && git rev-parse --short HEAD"
if errorlevel 1 (
  echo [ERROR] Rollback reset failed.
  pause
  exit /b 1
)
echo [OK] Server repository rolled back.

echo [2/3] Restart service...
ssh %SSH_OPTS% !SERVER! "sudo -n systemctl restart !SERVICE!"
if errorlevel 1 (
  echo [ERROR] Service restart failed after rollback.
  pause
  exit /b 1
)
echo [OK] Restart command sent.

echo [3/3] Wait for health...
ssh %SSH_OPTS% !SERVER! "timeout !RESTART_TIMEOUT_SEC! bash -lc 'until curl -fsS -X GET !HEALTH_URL! -o /dev/null; do sleep 1; done'"
if errorlevel 1 (
  echo [ERROR] Health check failed after rollback.
  echo [INFO] Current service status:
  ssh %SSH_OPTS% !SERVER! "sudo -n systemctl --no-pager status !SERVICE! -n 40"
  pause
  exit /b 1
)
echo [DONE] Rollback completed successfully.
pause
exit /b 0

rem ---------- Deploy tree over SSH without GitHub (DNS/offline friendly) ----------
:localsync
cd /d "%~dp0.."
if errorlevel 1 (
  echo [ERROR] Cannot switch to repo directory.
  exit /b 1
)
echo ==============================================================
echo LOCAL SYNC ^(no GitHub - SSH only^)
echo SERVER=!SERVER!
echo REMOTE_PATH=!REMOTE_PATH!
echo SERVICE=!SERVICE!
echo ==============================================================
echo [WARN] Uses git archive of TRACKED files at CURRENT HEAD only.
echo [WARN] Uncommitted changes are NOT sent - git commit first.
echo [WARN] Files deleted in git may still exist on server ^(no delete step^).
echo [WARN] Does not replace venv / server-only files outside repo tree.
echo.
echo [INFO] SSH target: !SERVER!

git rev-parse HEAD >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Not a git repo or no HEAD.
  exit /b 1
)
for /f %%h in ('git rev-parse --short HEAD') do echo [INFO] Archiving HEAD %%h ...

echo [1/3] git archive HEAD ^| ssh tar extract into !REMOTE_PATH! ...
git archive --format=tar HEAD | ssh %SSH_OPTS% !SERVER! "mkdir -p !REMOTE_PATH! && tar -xf - -C !REMOTE_PATH!"
if errorlevel 1 (
  echo [ERROR] localsync archive or ssh tar failed.
  echo [HINT] Set MPINFORMER_SSH_TARGET if SSH target is wrong.
  exit /b 1
)
echo [OK] Files unpacked on server.

call :remote_prepare_runtime
if errorlevel 1 exit /b 1

echo [2/3] Restart service...
ssh %SSH_OPTS% !SERVER! "sudo -n systemctl restart !SERVICE!"
if errorlevel 1 (
  echo [ERROR] Service restart failed.
  exit /b 1
)
echo [OK] Restart command sent.

echo [3/3] Wait for health...
ssh %SSH_OPTS% !SERVER! "timeout !RESTART_TIMEOUT_SEC! bash -lc 'until curl -fsS -X GET !HEALTH_URL! -o /dev/null; do sleep 1; done'"
if errorlevel 1 (
  echo [ERROR] Health check failed after localsync.
  ssh %SSH_OPTS% !SERVER! "sudo -n systemctl --no-pager status !SERVICE! -n 40"
  exit /b 1
)
echo [OK] localsync done; service healthy on !HEALTH_URL!.
exit /b 0

:main
cd /d "%~dp0.."
if errorlevel 1 (
  echo [ERROR] Cannot switch to repo directory.
  exit /b 1
)

echo ==============================================================
echo AUTO PUSH + DEPLOY ^(MPInformer VPS defaults^)
echo GIT_BRANCH=!GIT_BRANCH!
echo SERVER=!SERVER!
echo REMOTE_PATH=!REMOTE_PATH!
echo SERVICE=!SERVICE!
echo HEALTH_URL=!HEALTH_URL!
echo PIP_ON_SERVER=^(skip if DEPLOY_SKIP_PIP=1^)
echo TIMEOUT=!RESTART_TIMEOUT_SEC!s
echo SSH_OPTS=%SSH_OPTS%
echo ==============================================================
echo.

echo [1/8] Check local changes...
git status --short
if errorlevel 1 (
  echo [ERROR] git status failed.
  exit /b 1
)
echo [OK] git status done.
echo.

echo [2/8] Build auto commit message...
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd HH:mm:ss\""') do set "NOW_TS=%%i"
set "COMMIT_MSG=auto-deploy: !NOW_TS!"
echo [OK] Commit message: !COMMIT_MSG!
echo.

echo [3/8] git add -A ...
git add -A
if errorlevel 1 (
  echo [ERROR] git add failed.
  exit /b 1
)
echo [OK] git add done.
echo.

echo [4/8] git commit ...
git commit -m "!COMMIT_MSG!"
if errorlevel 1 (
  echo [WARN] No new commit ^(nothing to commit - working tree clean is usual^).
  echo [INFO] Continuing with git push anyway ^(sends any earlier local commits^).
) else (
  echo [OK] Commit created.
)
echo.

echo [5a/8] git fetch + count commits not yet on GitHub...
git fetch origin
if errorlevel 1 (
  echo [WARN] git fetch failed - check network and origin remote.
  echo [HINT] If message contains "Could not resolve hostname github.com" - DNS on THIS PC.
  echo [HINT] Try: ping github.com    nslookup github.com    ^(DNS 8.8.8.8 / 1.1.1.1^)
  echo [HINT] Deploy without GitHub ^(same LAN SSH only^):  scripts\push-and-deploy.bat localsync
)
for /f %%n in ('git rev-list --count origin/!GIT_BRANCH!..HEAD 2^>nul') do set "AHEAD_COUNT=%%n"
if not defined AHEAD_COUNT set "AHEAD_COUNT=0"
if "!AHEAD_COUNT!"=="0" (
  echo [INFO] No local-only commits vs origin/!GIT_BRANCH! ^(or branch not tracking^).
) else (
  echo [INFO] Commits on PC but not on GitHub yet: !AHEAD_COUNT! ^(need successful push^).
)

echo [5b/8] git push origin !GIT_BRANCH! ...
git push origin !GIT_BRANCH!
if errorlevel 1 (
  echo [ERROR] git push failed.
  echo [HINT] If "Could not resolve hostname github.com" - fix DNS / internet on THIS PC ^(not the VPS^).
  echo [HINT] Bypass GitHub ^(SSH archive HEAD -^> server^):  scripts\push-and-deploy.bat localsync !SERVER!
  echo [HINT] GitHub SSH key issues ^(not DNS^):
  echo [HINT]   1^) ssh -T git@github.com
  echo [HINT]   2^) Host github.com + IdentityFile in %%USERPROFILE%%\.ssh\config
  echo [HINT]   3^) ssh-add path\to\github_private_key
  echo [HINT] Or HTTPS+PAT: git remote set-url origin https://github.com/USER/REPO.git
  exit /b 1
)

echo [5c/8] Verify: after push, HEAD must match origin/!GIT_BRANCH!...
git fetch origin
for /f %%a in ('git rev-parse HEAD') do set "VERIFY_HEAD=%%a"
for /f %%b in ('git rev-parse origin/!GIT_BRANCH!') do set "VERIFY_ORIGIN=%%b"
if "!VERIFY_HEAD!"=="!VERIFY_ORIGIN!" (
  echo [OK] GitHub has commits: HEAD eq origin/!GIT_BRANCH!.
) else (
  echo [ERROR] After push, HEAD still differs from origin/!GIT_BRANCH!.
  echo [ERROR] Server cannot deploy your commits until this matches.
  echo [INFO]  HEAD local : !VERIFY_HEAD!
  echo [INFO]  origin/!GIT_BRANCH!: !VERIFY_ORIGIN!
  exit /b 1
)
echo [OK] Push verified against origin/!GIT_BRANCH!.
for /f %%h in ('git rev-parse --short HEAD') do set "LOCAL_HEAD_SHORT=%%h"
for /f %%f in ('git rev-parse HEAD') do set "LOCAL_HEAD_FULL=%%f"
echo [INFO] Local HEAD: !LOCAL_HEAD_SHORT! ^(!LOCAL_HEAD_FULL!^)
git log -1 --oneline
echo.

echo [6/8] Server: stash, hard-sync to origin/!GIT_BRANCH!, verify ...
echo [INFO] VERIFY "HEAD now" on server must equal PC hash: !LOCAL_HEAD_FULL!
echo [INFO] systemd WorkingDirectory must match repo: !REMOTE_PATH!
echo [INFO] Uses: checkout -B !GIT_BRANCH! origin/!GIT_BRANCH! + reset --hard ^(after stash^).
echo [INFO] SSH target: !SERVER!
ssh %SSH_OPTS% !SERVER! bash -lc "mkdir -p '%REMOTE_PATH%' && cd '%REMOTE_PATH%' && if [ ! -d .git ]; then git init && git remote add origin '%ORIGIN_URL%'; fi && git fetch origin '%GIT_BRANCH%' && (git stash push -u -m auto-deploy-pre-pull || true) && git checkout -B '%GIT_BRANCH%' 'origin/%GIT_BRANCH%' && git reset --hard 'origin/%GIT_BRANCH%'"
if errorlevel 1 (
  echo [WARN] Server git sync failed ^(fetch/checkout/reset^).
  echo [WARN] Possible reason: server has no internet/DNS to GitHub.
  echo [INFO] Fallback to LOCALSYNC deploy to the same SSH target...
  call :localsync
  exit /b !ERRORLEVEL!
)
ssh %SSH_OPTS% !SERVER! bash -lc "bash !REMOTE_PATH!/scripts/server-pull-verify.sh !REMOTE_PATH! !GIT_BRANCH! !SERVICE!"
if errorlevel 1 (
  echo [ERROR] Server verify step failed - see SSH output above ^(VERIFY block^).
  echo [HINT] ssh %SSH_OPTS% !SERVER!
  echo [HINT] On server: cd !REMOTE_PATH! ^&^& git status ^&^& git remote -v ^&^& git stash list
  echo [HINT] systemctl show !SERVICE! -p WorkingDirectory -p ExecStart
  echo [HINT] If ExecStart not under !REMOTE_PATH!, fix unit file then daemon-reload + restart.
  exit /b 1
)
echo [OK] Server pull + verify done.
echo [INFO] Match VERIFY HEAD with PC hash !LOCAL_HEAD_FULL!
echo [INFO] Server edits may be in: git stash list ^(on server^)
echo.

echo [7/8] Server: uploads dir + pip install ^(unless DEPLOY_SKIP_PIP=1^) ...
call :remote_prepare_runtime
if errorlevel 1 exit /b 1

echo [8/8] Restart service and wait for health GET !HEALTH_URL! ...
ssh %SSH_OPTS% !SERVER! "sudo -n systemctl restart !SERVICE!"
if errorlevel 1 (
  echo [ERROR] Service restart failed.
  echo [HINT] Add sudo NOPASSWD for systemctl restart/status !SERVICE! for this user.
  exit /b 1
)
echo [OK] Restart command sent.

echo [INFO] Waiting for health GET timeout !RESTART_TIMEOUT_SEC!s ...
ssh %SSH_OPTS% !SERVER! "timeout !RESTART_TIMEOUT_SEC! bash -lc 'until curl -fsS -X GET !HEALTH_URL! -o /dev/null; do sleep 1; done'"
if errorlevel 1 (
  echo [ERROR] Restart timeout reached.
  echo [INFO] Current service status:
  ssh %SSH_OPTS% !SERVER! "sudo -n systemctl --no-pager status !SERVICE! -n 40"
  exit /b 1
)

echo [OK] Service is healthy on !HEALTH_URL!.
exit /b 0

rem ---------- Remote venv + uploads (after git sync or localsync) ----------
:remote_prepare_runtime
ssh %SSH_OPTS% !SERVER! bash -lc "mkdir -p '%REMOTE_PATH%/uploads'"
if errorlevel 1 (
  echo [ERROR] mkdir uploads on server failed.
  exit /b 1
)
if /I "!DEPLOY_SKIP_PIP!"=="1" (
  echo [SKIP] DEPLOY_SKIP_PIP=1 — pip install on server skipped.
  exit /b 0
)
echo [INFO] Server: ensure venv + pip install -r requirements.txt
ssh %SSH_OPTS% !SERVER! bash -lc "REQ_FILE='%REMOTE_PATH%/requirements.txt'; HASH_FILE='%REMOTE_PATH%/.deploy-requirements.sha256'; CUR_HASH=\$(sha256sum \"\$REQ_FILE\" | awk '{print \$1}'); NEED_PIP=0; if [ ! -x '%REMOTE_PATH%/venv/bin/python' ]; then NEED_PIP=1; fi; if [ ! -f \"\$HASH_FILE\" ]; then NEED_PIP=1; else OLD_HASH=\$(cat \"\$HASH_FILE\" 2>/dev/null || true); if [ \"\$OLD_HASH\" != \"\$CUR_HASH\" ]; then NEED_PIP=1; fi; fi; if [ \"\$NEED_PIP\" = \"1\" ]; then python3 -m venv '%REMOTE_PATH%/venv' && '%REMOTE_PATH%/venv/bin/python' -m pip install -r \"\$REQ_FILE\" && printf \"%s\" \"\$CUR_HASH\" > \"\$HASH_FILE\" && echo [OK] pip install executed; else echo [SKIP] requirements unchanged, pip skipped; fi"
if errorlevel 1 (
  echo [ERROR] pip install on server failed.
  exit /b 1
)
echo [OK] pip install on server completed.
exit /b 0
