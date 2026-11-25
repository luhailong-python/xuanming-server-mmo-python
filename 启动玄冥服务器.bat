@echo off
REM File encoding: UTF-8 (ASCII-safe REM lines for Windows cmd).
setlocal

REM Anchor all relative runtime paths to the launcher directory.
pushd "%~dp0"
if errorlevel 1 (
    echo [ERROR] Cannot enter the server directory.
    pause
    exit /b 1
)

set "PYEXE="
set "PYROOT="

REM Prefer the repository virtual environment first, then py launcher, then python.
if exist "%~dp0.venv\Scripts\python.exe" (
    set "PYEXE=%~dp0.venv\Scripts\python.exe"
    set "PYROOT=%~dp0.venv\Scripts"
)
if not defined PYEXE (
    where py >nul 2>nul
    if not errorlevel 1 set "PYEXE=py"
)
if not defined PYEXE (
    where python >nul 2>nul
    if not errorlevel 1 set "PYEXE=python"
)
if not defined PYEXE (
    echo [ERROR] Python not found.
    echo         Create .venv or install Python 3.11+ on PATH.
    pause
    popd
    exit /b 1
)

REM Require Python 3.11+.
"%PYEXE%" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Selected Python must be 3.11 or newer: %PYEXE%
    pause
    popd
    exit /b 1
)

if defined PYROOT set "PATH=%PYROOT%;%PATH%"
if not defined PYTHONUTF8 set "PYTHONUTF8=1"
if not defined PYTHONIOENCODING set "PYTHONIOENCODING=utf-8"

echo [python] executable = "%PYEXE%"
"%PYEXE%" --version
if /i "%~1"=="--check-python" (
    popd
    exit /b 0
)

REM Provide default local dev login token when unset.
if not defined XUANMING_DEV_LOGIN_TOKEN set "XUANMING_DEV_LOGIN_TOKEN=dev-local-token"

REM Redirect server stdout/stderr to a log file so console repainting does not steal CPU/GPU.
if not exist "%~dp0run\logs" mkdir "%~dp0run\logs"
set "CONSOLE_LOG=%~dp0run\logs\server-console.log"

echo [%date% %time%] Starting Xuanming local server...
echo [log] output = "%CONSOLE_LOG%"
echo [log] follow = powershell -NoProfile -Command "Get-Content -Wait -Tail 50 -LiteralPath '%CONSOLE_LOG%'"

"%PYEXE%" -m xuanming run local --config config\local.toml > "%CONSOLE_LOG%" 2>&1
set EXIT_CODE=%ERRORLEVEL%

if not "%EXIT_CODE%"=="0" (
    echo.
    echo [log] last 40 lines of "%CONSOLE_LOG%":
    powershell -NoProfile -Command "Get-Content -Tail 40 -LiteralPath '%CONSOLE_LOG%'"
)

echo.
echo [%date% %time%] Xuanming server exited with code %EXIT_CODE%.
if not "%EXIT_CODE%"=="0" (
    echo [ERROR] Server exited with code %EXIT_CODE%. See the log above.
    pause
)
popd
exit /b %EXIT_CODE%
