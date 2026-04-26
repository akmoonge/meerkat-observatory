@echo off
title Meerkat's Observatory - Setup
echo.
echo  ===========================
echo   Meerkat's Observatory Setup
echo  ===========================
echo.

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [!] Python not found.
    echo [!] Opening download page...
    echo.
    echo     1. Install Python 3.10+
    echo     2. CHECK "Add Python to PATH"
    echo     3. Run this file again
    echo.
    start https://www.python.org/downloads/
    pause
    exit /b
)

echo [OK] Python found:
python --version
echo.

:: Install packages
echo [..] Installing packages...
echo.
python -m pip install --upgrade pip
python -m pip install streamlit pandas numpy plotly yfinance fredapi pykrx fastdtw scipy pyarrow

if errorlevel 1 (
    echo.
    echo [!] Install failed. Check errors above.
    pause
    exit /b
)

echo.
echo  ===========================
echo   Setup complete!
echo   Run meerkat_observatory.bat to start
echo  ===========================
echo.
pause
