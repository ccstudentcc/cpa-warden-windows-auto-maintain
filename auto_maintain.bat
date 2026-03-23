@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%" >nul
set "LAUNCHER_STATE_DIR=%SCRIPT_DIR%.auto_maintain_state"
set "LAUNCHER_LOCK_FILE=%LAUNCHER_STATE_DIR%\auto_maintain_launcher.lock"

if not exist "%SCRIPT_DIR%auto_maintain.py" (
    echo [ERROR] auto_maintain.py not found in %SCRIPT_DIR%
    exit /b 1
)

if not exist "%LAUNCHER_STATE_DIR%" mkdir "%LAUNCHER_STATE_DIR%" >nul 2>nul

if exist "%LAUNCHER_LOCK_FILE%" (
    set "EXISTING_PID="
    for /f "usebackq delims=" %%I in ("%LAUNCHER_LOCK_FILE%") do (
        set "EXISTING_PID=%%I"
    )
    if defined EXISTING_PID (
        python -c "import os,sys; os.kill(int(sys.argv[1]), 0)" "!EXISTING_PID!" >nul 2>nul
        if not errorlevel 1 (
            echo [ERROR] Another auto_maintain launcher is already running. pid=!EXISTING_PID!
            exit /b 1
        )
    )
    del /f /q "%LAUNCHER_LOCK_FILE%" >nul 2>nul
)

set "LAUNCHER_PID="
for /f "usebackq delims=" %%I in (`python -c "import os; print(os.getppid())"`) do (
    set "LAUNCHER_PID=%%I"
)
if not defined LAUNCHER_PID set "LAUNCHER_PID=unknown"
> "%LAUNCHER_LOCK_FILE%" echo %LAUNCHER_PID%

if not exist "%LAUNCHER_LOCK_FILE%" (
    echo [ERROR] Failed to create launcher lock file: %LAUNCHER_LOCK_FILE%
    exit /b 1
)

set "ALLOW_MULTI_INSTANCE=0"

if "%AUTO_MAINTAIN_FORCE_PYTHON%"=="1" (
    python "%SCRIPT_DIR%auto_maintain.py" %*
    set "EXIT_CODE=%ERRORLEVEL%"
    goto :finalize
)

where uv >nul 2>nul
if errorlevel 1 (
    echo [WARN] uv not found, fallback to python.
    python "%SCRIPT_DIR%auto_maintain.py" %*
    set "EXIT_CODE=%ERRORLEVEL%"
    goto :finalize
)

uv run python "%SCRIPT_DIR%auto_maintain.py" %*
if errorlevel 1 (
    echo [WARN] uv run failed, fallback to python.
    python "%SCRIPT_DIR%auto_maintain.py" %*
    set "EXIT_CODE=%ERRORLEVEL%"
    goto :finalize
)

set "EXIT_CODE=0"

:finalize
if exist "%LAUNCHER_LOCK_FILE%" del /f /q "%LAUNCHER_LOCK_FILE%" >nul 2>nul
exit /b %EXIT_CODE%
