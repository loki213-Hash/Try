@echo off
title Multi-Index Options Signal Bot
cd /d "%~dp0"

echo ========================================================
echo   Multi-Index Indian Options Signal Bot
echo   Paper Trading and Signal Service
echo ========================================================
echo.

if not exist ".venv\Scripts\python.exe" (
    echo Error: Virtual environment not found at .venv
    echo Please install dependencies first.
    pause
    exit /b 1
)

echo Activating environment and launching server...
echo Web Dashboard will be available at: http://localhost:8000/
echo.

rem Release port 8000 if an orphan instance from a prior run is still listening
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8000" ^| findstr "LISTENING"') do (
    echo [Port 8000 occupied by PID %%a] Terminating orphan process...
    taskkill /F /PID %%a >nul 2>&1
)

call .\.venv\Scripts\activate.bat
python main.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Server stopped with error code %ERRORLEVEL%.
    pause
)
