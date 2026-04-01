@echo off
echo ============================================================
echo  AI Pre-Market Gappers Screener
echo ============================================================

REM Load variables from .env file
for /f "tokens=1,2 delims==" %%a in (.env) do set %%a=%%b

if "%APP_HOST%"=="" set APP_HOST=127.0.0.1
if "%APP_PORT%"=="" set APP_PORT=5000

echo OpenRouter model: %OPENROUTER_MODEL%
echo Starting Flask at http://%APP_HOST%:%APP_PORT% ...
echo Press Ctrl+C to stop.
echo.

cd /d "%~dp0"
python app.py

pause
