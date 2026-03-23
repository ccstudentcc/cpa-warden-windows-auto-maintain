@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "SCRIPT_DIR=%~dp0"
pushd "%SCRIPT_DIR%" >nul

set "INSTANCE_TS="
for /f "usebackq delims=" %%I in (`python -c "import datetime; print(datetime.datetime.now().strftime('%%Y-%%m-%%d %%H:%%M:%%S'))"`) do (
    echo(%%I| findstr /R "^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]$" >nul
    if not errorlevel 1 set "INSTANCE_TS=%%I"
)
if not defined INSTANCE_TS set "INSTANCE_TS=unknown-time"

set "INSTANCE_PID="
for /f "usebackq delims=" %%I in (`python -c "import os; print(os.getppid())"`) do (
    echo(%%I| findstr /R "^[0-9][0-9]*$" >nul
    if not errorlevel 1 set "INSTANCE_PID=%%I"
)
if not defined INSTANCE_PID set "INSTANCE_PID=unknown-pid"

set "INSTANCE_LABEL=cpa-auto-maintain | !INSTANCE_TS! | !INSTANCE_PID!"
title cpa-auto-maintain ^| !INSTANCE_TS! ^| !INSTANCE_PID!

rem ===== One-click profile (edit values as needed) =====
set "AUTH_DIR=%SCRIPT_DIR%auth_files"
set "CONFIG_PATH=%SCRIPT_DIR%config.json"
if not exist "%CONFIG_PATH%" (
    echo [WARN] config.json not found at %CONFIG_PATH%
    echo [WARN] Fallback to default config resolution.
    set "CONFIG_PATH="
)

rem Maintain every 1 hour to reduce API and local load.
set "MAINTAIN_INTERVAL_SECONDS=3600"

rem Poll every 30 seconds (lower CPU vs 5~10s polling).
set "WATCH_INTERVAL_SECONDS=30"

rem Wait 10 seconds after change detection to avoid repeated uploads during batch copy.
set "UPLOAD_STABLE_WAIT_SECONDS=10"

rem When file count is unchanged, perform deep snapshot every 120 loops (about 60 minutes with 30s polling).
set "DEEP_SCAN_INTERVAL_LOOPS=120"

set "RUN_MAINTAIN_ON_START=1"
set "RUN_UPLOAD_ON_START=1"
set "RUN_MAINTAIN_AFTER_UPLOAD=1"
set "DELETE_UPLOADED_FILES_AFTER_UPLOAD=1"

rem Retry failed maintain/upload once more with 20s delay.
set "MAINTAIN_RETRY_COUNT=1"
set "UPLOAD_RETRY_COUNT=1"
set "COMMAND_RETRY_DELAY_SECONDS=20"

rem Keep watcher running even if a command fails.
set "CONTINUE_ON_COMMAND_FAILURE=1"

rem Prevent duplicate watcher instances on the same state directory.
set "ALLOW_MULTI_INSTANCE=0"

rem ZIP handling: detect zip, auto-extract JSON archives with Bandizip, then delete source zip.
set "INSPECT_ZIP_FILES=1"
set "AUTO_EXTRACT_ZIP_JSON=1"
set "DELETE_ZIP_AFTER_EXTRACT=1"
set "BANDIZIP_PATH=D:\Bandizp\Bandizip.exe"
set "BANDIZIP_TIMEOUT_SECONDS=120"
set "USE_WINDOWS_ZIP_FALLBACK=1"

rem Store watcher state in a dedicated directory.
set "STATE_DIR=%SCRIPT_DIR%.auto_maintain_state"
if not exist "%STATE_DIR%" mkdir "%STATE_DIR%" >nul 2>nul

rem Duplicate-start guard: skip launch if an instance with same STATE_DIR is running.
set "LOCK_FILE=%STATE_DIR%\auto_maintain.lock"
if exist "%LOCK_FILE%" (
    set "LOCK_PID="
    set "LOCK_TOKEN="
    set "LOCK_STARTED_UNIX="
    set "LOCK_LINE="
    set /p LOCK_LINE=<"%LOCK_FILE%"
    for /f "tokens=1,2,3 delims=|" %%P in ("!LOCK_LINE!") do (
        if not defined LOCK_PID set "LOCK_PID=%%P"
        if not defined LOCK_TOKEN set "LOCK_TOKEN=%%Q"
        if not defined LOCK_STARTED_UNIX set "LOCK_STARTED_UNIX=%%R"
    )

    set "PID_RUNNING=0"
    if defined LOCK_PID (
        echo(!LOCK_PID!| findstr /R "^[0-9][0-9]*$" >nul
        if not errorlevel 1 (
            set "TASK_LINE="
            for /f "usebackq delims=" %%L in (`tasklist /FI "PID eq !LOCK_PID!" /FO CSV /NH 2^>nul`) do (
                if not defined TASK_LINE set "TASK_LINE=%%L"
            )
            if defined TASK_LINE (
                if /I "!TASK_LINE:~0,5!"=="INFO:" (
                    set "PID_RUNNING=0"
                ) else (
                    set "PID_RUNNING=1"
                )
            )
        )
    )

    if "!PID_RUNNING!"=="1" (
        echo [INFO] auto_maintain is already running. Skip duplicate launch.
        echo [INFO] lock: pid=!LOCK_PID! token=!LOCK_TOKEN! started_unix=!LOCK_STARTED_UNIX!
        exit /b 0
    )

    echo [WARN] stale lock detected, removing: %LOCK_FILE%
    echo [WARN] stale lock detail: pid=!LOCK_PID! token=!LOCK_TOKEN! started_unix=!LOCK_STARTED_UNIX!
    del /q "%LOCK_FILE%" >nul 2>nul
)

echo Starting optimized auto-maintain profile...
echo INSTANCE_LABEL=!INSTANCE_LABEL!
echo INSTANCE_LOCK_FILE=%LOCK_FILE%
echo AUTH_DIR=%AUTH_DIR%
echo MAINTAIN_INTERVAL_SECONDS=%MAINTAIN_INTERVAL_SECONDS%
echo WATCH_INTERVAL_SECONDS=%WATCH_INTERVAL_SECONDS%
echo UPLOAD_STABLE_WAIT_SECONDS=%UPLOAD_STABLE_WAIT_SECONDS%
echo DEEP_SCAN_INTERVAL_LOOPS=%DEEP_SCAN_INTERVAL_LOOPS%
echo MAINTAIN_RETRY_COUNT=%MAINTAIN_RETRY_COUNT%
echo UPLOAD_RETRY_COUNT=%UPLOAD_RETRY_COUNT%
echo COMMAND_RETRY_DELAY_SECONDS=%COMMAND_RETRY_DELAY_SECONDS%
echo CONTINUE_ON_COMMAND_FAILURE=%CONTINUE_ON_COMMAND_FAILURE%
echo ALLOW_MULTI_INSTANCE=%ALLOW_MULTI_INSTANCE%
echo DELETE_UPLOADED_FILES_AFTER_UPLOAD=%DELETE_UPLOADED_FILES_AFTER_UPLOAD%
echo INSPECT_ZIP_FILES=%INSPECT_ZIP_FILES%
echo AUTO_EXTRACT_ZIP_JSON=%AUTO_EXTRACT_ZIP_JSON%
echo DELETE_ZIP_AFTER_EXTRACT=%DELETE_ZIP_AFTER_EXTRACT%
echo BANDIZIP_PATH=%BANDIZIP_PATH%
echo BANDIZIP_TIMEOUT_SECONDS=%BANDIZIP_TIMEOUT_SECONDS%
echo USE_WINDOWS_ZIP_FALLBACK=%USE_WINDOWS_ZIP_FALLBACK%
echo.
echo Stop with Ctrl+C.
echo.

call "%SCRIPT_DIR%auto_maintain.bat" %*
exit /b %ERRORLEVEL%
