@echo off
title Multi-Index Options Signal Bot - Test Suite
cd /d "%~dp0"

echo Running all 20 automated tests...
call .\.venv\Scripts\activate.bat
pytest tests -v

echo.
pause
