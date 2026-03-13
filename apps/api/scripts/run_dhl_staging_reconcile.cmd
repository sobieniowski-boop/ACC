@echo off
setlocal
cd /d C:\ACC\apps\api
set PYTHONPATH=C:\ACC\apps\api
"C:\ACC\.venv\Scripts\python.exe" "C:\ACC\apps\api\scripts\run_dhl_staging_reconcile.py" %*
