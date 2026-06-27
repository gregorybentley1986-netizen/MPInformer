@echo off
rem Single entrypoint: PC -> GitHub -> VPS (git + pip) -> restart mpinformer -> wait /health -> exit code.
rem Prerequisites: Git SSH to GitHub on PC; SSH PC->VPS; deploy key VPS->GitHub; sudo systemctl without password on VPS if not root.
rem
rem Set VPS host once (pick one):
rem   setx MPINFORMER_SSH_TARGET "root@YOUR_IP"
rem Or uncomment the next line in this file:
rem set "MPINFORMER_SSH_TARGET=root@2.56.240.226"
rem
rem Optional:
rem   set DEPLOY_HEALTH_URL=http://127.0.0.1:8001/health
rem   set DEPLOY_SKIP_PIP=0   force pip on server (default: skip unless requirements.txt changed)
rem   set DEPLOY_NO_PAUSE=1
rem Usage:
rem   scripts\deploy-to-vps.bat
rem   scripts\deploy-to-vps.bat rollback
rem   scripts\deploy-to-vps.bat  root@backup-host
if not defined DEPLOY_NO_PAUSE set "DEPLOY_NO_PAUSE=1"
if not defined DEPLOY_SKIP_PIP call "%~dp0deploy-detect-pip.bat"
call "%~dp0push-and-deploy.bat" %*
set "DEPLOY_EC=%ERRORLEVEL%"
exit /b %DEPLOY_EC%
