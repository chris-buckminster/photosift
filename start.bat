@echo off
echo.
echo  Starting PhotoSift...
echo.
call venv\Scripts\activate.bat 2>nul
if errorlevel 1 (
    echo  [ERROR] Run setup.bat first.
    pause
    exit /b 1
)
python run.py %*
