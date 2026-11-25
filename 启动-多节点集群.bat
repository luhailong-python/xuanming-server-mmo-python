@echo off
REM ============================================================
REM  CLUSTER profile - pure-Python multi-node cluster
REM  Requires MySQL/Redis/etcd/Kafka already running.
REM  Starts all nodes in parallel via tools/supervisor.py (pure stdlib).
REM    - preflight requires check for MySQL/Redis/etcd/Kafka
REM    - parallel spawn with real TCP port readiness probe
REM    - per-process log under run/logs/
REM    - watchdog with exponential backoff restart
REM  Process table: config/processes.cluster.json
REM ============================================================
setlocal

pushd "%~dp0"
if errorlevel 1 (
    echo [ERROR] Cannot enter the server directory.
    pause
    exit /b 1
)

set "PYEXE="
if exist "%~dp0.venv\Scripts\python.exe" set "PYEXE=%~dp0.venv\Scripts\python.exe"
if not defined PYEXE where py >nul 2>nul && set "PYEXE=py"
if not defined PYEXE where python >nul 2>nul && set "PYEXE=python"
if not defined PYEXE (
    echo [ERROR] No Python found.
    echo         Create .venv or install Python 3.11+ on PATH.
    pause
    popd
    exit /b 1
)

"%PYEXE%" "%~dp0tools\supervisor.py" --profile cluster %*
set EXIT_CODE=%ERRORLEVEL%
echo.
echo supervisor exited, code %EXIT_CODE%
pause
popd
exit /b %EXIT_CODE%
