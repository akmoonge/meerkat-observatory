@echo off
title Meerkat's Observatory
cd /d "%~dp0"
python -m streamlit run meerkat_observatory.py
pause
