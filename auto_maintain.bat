@echo off
setlocal EnableExtensions

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%" >nul

if not exist "%SCRIPT_DIR%auto_maintain.py" (
    echo [ERROR] auto_maintain.py not found in %SCRIPT_DIR%
    exit /b 1
)

if "%AUTO_MAINTAIN_FORCE_PYTHON%"=="1" (
    python "%SCRIPT_DIR%auto_maintain.py" %*
    exit /b %ERRORLEVEL%
)

where uv >nul 2>nul
if errorlevel 1 (
    echo [WARN] uv not found, fallback to python.
    python "%SCRIPT_DIR%auto_maintain.py" %*
    exit /b %ERRORLEVEL%
)

uv run python "%SCRIPT_DIR%auto_maintain.py" %*
if errorlevel 1 (
    echo [WARN] uv run failed, fallback to python.
    python "%SCRIPT_DIR%auto_maintain.py" %*
    exit /b %ERRORLEVEL%
)

exit /b 0
