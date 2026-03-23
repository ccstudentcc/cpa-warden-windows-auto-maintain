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

set "WATCH_CONFIG_PATH=%SCRIPT_DIR%auto_maintain.config.json"
set "WATCH_CONFIG_EXAMPLE=%SCRIPT_DIR%auto_maintain.config.example.json"
set "USE_WATCH_CONFIG=1"

if not exist "%WATCH_CONFIG_PATH%" (
    if exist "%WATCH_CONFIG_EXAMPLE%" (
        copy /Y "%WATCH_CONFIG_EXAMPLE%" "%WATCH_CONFIG_PATH%" >nul
        if exist "%WATCH_CONFIG_PATH%" (
            echo [INFO] Created %WATCH_CONFIG_PATH% from example.
        ) else (
            echo [WARN] Failed to create %WATCH_CONFIG_PATH% from example.
            set "USE_WATCH_CONFIG=0"
        )
    ) else (
        echo [WARN] %WATCH_CONFIG_PATH% not found and no example available.
        echo [WARN] Fallback to env/default-only watcher settings.
        set "USE_WATCH_CONFIG=0"
    )
)

set "CONFIG_PATH=%SCRIPT_DIR%config.json"
if not exist "%CONFIG_PATH%" (
    echo [WARN] config.json not found at %CONFIG_PATH%
    echo [WARN] cpa_warden.py will fallback to default config resolution.
)

echo Starting optimized auto-maintain profile...
echo INSTANCE_LABEL=!INSTANCE_LABEL!
echo USE_WATCH_CONFIG=%USE_WATCH_CONFIG%
if "%USE_WATCH_CONFIG%"=="1" echo WATCH_CONFIG_PATH=%WATCH_CONFIG_PATH%
echo.
echo Stop with Ctrl+C.
echo.

if "%USE_WATCH_CONFIG%"=="1" (
    call "%SCRIPT_DIR%auto_maintain.bat" --watch-config "%WATCH_CONFIG_PATH%" %*
) else (
    call "%SCRIPT_DIR%auto_maintain.bat" %*
)
exit /b %ERRORLEVEL%
