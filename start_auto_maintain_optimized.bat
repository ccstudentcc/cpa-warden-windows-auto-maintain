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

rem Maintain every 40 minutes to reduce API and local load.
set "MAINTAIN_INTERVAL_SECONDS=2400"

rem Poll every 30 seconds (lower CPU vs 5~10s polling).
set "WATCH_INTERVAL_SECONDS=30"

rem Wait 10 seconds after change detection to avoid repeated uploads during batch copy.
set "UPLOAD_STABLE_WAIT_SECONDS=10"

rem When file count is unchanged, perform deep snapshot every 120 loops (about 40 minutes with 30s polling).
set "DEEP_SCAN_INTERVAL_LOOPS=120"

set "RUN_MAINTAIN_ON_START=1"
set "RUN_UPLOAD_ON_START=1"
set "RUN_MAINTAIN_AFTER_UPLOAD=1"
rem Set to 1 for unattended mode. Set to 0 if you want maintain confirmations.
set "MAINTAIN_ASSUME_YES=1"
set "DELETE_UPLOADED_FILES_AFTER_UPLOAD=1"

rem Retry failed maintain/upload once more with 20s delay.
set "MAINTAIN_RETRY_COUNT=1"
set "UPLOAD_RETRY_COUNT=1"
set "COMMAND_RETRY_DELAY_SECONDS=20"

rem Fail fast by default if maintain/upload command fails.
set "CONTINUE_ON_COMMAND_FAILURE=0"

rem Prevent duplicate watcher instances on the same state directory.
set "ALLOW_MULTI_INSTANCE=0"

rem ZIP handling: detect zip, auto-extract JSON archives with Bandizip, then delete source zip.
set "INSPECT_ZIP_FILES=1"
set "AUTO_EXTRACT_ZIP_JSON=1"
set "DELETE_ZIP_AFTER_EXTRACT=1"
if "%BANDIZIP_PATH%"=="" set "BANDIZIP_PATH=D:\Bandizp\Bandizip.exe"
set "BANDIZIP_TIMEOUT_SECONDS=120"
set "USE_WINDOWS_ZIP_FALLBACK=1"

rem Store watcher state in a dedicated directory.
set "STATE_DIR=%SCRIPT_DIR%.auto_maintain_state"
if not exist "%STATE_DIR%" mkdir "%STATE_DIR%" >nul 2>nul
set "MAINTAIN_DB_PATH=%STATE_DIR%\cpa_warden_maintain.sqlite3"
set "UPLOAD_DB_PATH=%STATE_DIR%\cpa_warden_upload.sqlite3"
set "MAINTAIN_LOG_FILE=%STATE_DIR%\cpa_warden_maintain.log"
set "UPLOAD_LOG_FILE=%STATE_DIR%\cpa_warden_upload.log"

set "LOCK_FILE=%STATE_DIR%\auto_maintain.lock"

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
echo MAINTAIN_ASSUME_YES=%MAINTAIN_ASSUME_YES%
echo MAINTAIN_DB_PATH=%MAINTAIN_DB_PATH%
echo UPLOAD_DB_PATH=%UPLOAD_DB_PATH%
echo MAINTAIN_LOG_FILE=%MAINTAIN_LOG_FILE%
echo UPLOAD_LOG_FILE=%UPLOAD_LOG_FILE%
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
