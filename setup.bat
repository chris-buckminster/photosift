@echo off
echo.
echo  ========================================
echo   PhotoSift - Setup
echo  ========================================
echo.
python --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python is not installed or not in PATH.
    echo  Please install Python 3.10+ from https://python.org
    pause
    exit /b 1
)
echo  [1/3] Creating virtual environment...
python -m venv venv
echo  [2/3] Installing dependencies...
call venv\Scripts\activate.bat
pip install -r requirements.txt --quiet
echo  [3/3] Setup complete!
echo.
echo  To run PhotoSift: start.bat
echo.
pause
