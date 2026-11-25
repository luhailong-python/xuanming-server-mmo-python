@echo off
REM ============================================================
REM  LOCAL profile - JSON/SQLite local save, no MySQL/Redis/etcd/Kafka
REM  Starts via tools/supervisor.py (pure stdlib).
REM    - parallel spawn with bounded startup time
REM    - real TCP port readiness probe (Gate 127.0.0.1:8610)
REM    - per-process log under run/logs/
REM    - watchdog with exponential backoff restart
REM  Process table: config/processes.local.json
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

"%PYEXE%" "%~dp0tools\supervisor.py" --profile local %*
set EXIT_CODE=%ERRORLEVEL%
echo.
echo supervisor exited, code %EXIT_CODE%
pause
popd
exit /b %EXIT_CODE%
