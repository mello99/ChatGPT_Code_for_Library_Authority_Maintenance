@echo off
setlocal enableextensions enabledelayedexpansion

REM === CONFIGURATION ===
set "url=https://id.loc.gov/authorities/subjects/activitystreams/feed/1"
set "output_folder=C:\Scripts\YourFolder\LCSH_Activity_Streams"
set "log_folder=C:\Scripts\YourFolder\logs"

REM === Get stable date (locale-agnostic) ===
for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') do set "today=%%I"
set "filename=LCSH_activity_stream_!today!.json"
set "outfile=%output_folder%\%filename%"
set "logfile=%log_folder%\lcsh_download_log_!today!.txt"

REM === Ensure folders exist ===
if not exist "%output_folder%" mkdir "%output_folder%"
if not exist "%log_folder%" mkdir "%log_folder%"

REM === Validate PowerShell exists ===
where powershell >nul 2>&1
if errorlevel 1 (
    echo [%date% %time%] ERROR: PowerShell not found >> "%logfile%"
    exit /b 1
)

REM === Validate output write access ===
>nul 2>"%outfile%" (
    echo.
) || (
    echo [%date% %time%] ERROR: Cannot write to "%outfile%" >> "%logfile%"
    exit /b 1
)
del "%outfile%" >nul 2>&1

REM === Log start ===
echo [%date% %time%] Starting download to "%outfile%" >> "%logfile%"

REM === Download using PowerShell ===
powershell -NoProfile -Command ^
    "try { Invoke-WebRequest '%url%' -OutFile '%outfile%' -ErrorAction Stop } catch { Write-Error 'Download failed.'; exit 1 }"

if errorlevel 1 (
    echo [%date% %time%] ERROR: Download failed. >> "%logfile%"
    exit /b 1
)

REM === Log success ===
echo [%date% %time%] Download complete: "%outfile%" >> "%logfile%"
exit /b 0
