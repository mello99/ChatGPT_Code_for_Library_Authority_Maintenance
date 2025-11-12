@echo off
setlocal enabledelayedexpansion

rem --- Build YYYY-MM-DD (locale-independent) ---
for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') do set DATESTR=%%I

rem --- Configurable paths (edit as needed) ---
set "PY=C:\Path\To\Python\python.exe"
set "SCRIPT=C:\Scripts\LC_Activity_Streams_MARCXML_Downloader.py"
set "LOGDIR=C:\Scripts\logs"
set "LOGFILE=%LOGDIR%\marcxml_log_%DATESTR%.txt"

rem --- Validate paths exist ---
if not exist "%PY%" (
    echo [ERROR] Python not found at "%PY%"
    exit /b 1
)

if not exist "%SCRIPT%" (
    echo [ERROR] Script not found at "%SCRIPT%"
    exit /b 1
)

if not exist "%LOGDIR%" mkdir "%LOGDIR%"

rem --- Log start ---
echo [%DATE% %TIME%] Starting harvest... >> "%LOGFILE%" 2>&1

rem --- Run Python script and capture exit code ---
"%PY%" "%SCRIPT%" >> "%LOGFILE%" 2>&1
set "EXITCODE=!errorlevel!"

rem --- Log completion and exit code ---
echo [%DATE% %TIME%] Finished. Exit code: !EXITCODE! >> "%LOGFILE%" 2>&1

endlocal
exit /b %EXITCODE%
