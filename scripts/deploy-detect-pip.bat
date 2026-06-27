@echo off
rem Set DEPLOY_SKIP_PIP=1 unless requirements.txt is in the last commit or working tree.
setlocal EnableExtensions
set "DEPLOY_SKIP_PIP=1"
git diff --name-only HEAD~1 HEAD 2>nul | findstr /I /R /C:"^requirements.txt$" /C:"^requirements-dev.txt$" >nul
if not errorlevel 1 set "DEPLOY_SKIP_PIP=0"
git diff --name-only 2>nul | findstr /I /R /C:"^requirements.txt$" /C:"^requirements-dev.txt$" >nul
if not errorlevel 1 set "DEPLOY_SKIP_PIP=0"
git diff --cached --name-only 2>nul | findstr /I /R /C:"^requirements.txt$" /C:"^requirements-dev.txt$" >nul
if not errorlevel 1 set "DEPLOY_SKIP_PIP=0"
endlocal & set "DEPLOY_SKIP_PIP=%DEPLOY_SKIP_PIP%"
