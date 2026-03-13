@echo off
setlocal
cd /d C:\ACC\apps\api
set PYTHONPATH=C:\ACC\apps\api
"C:\ACC\.venv\Scripts\python.exe" -u "C:\ACC\apps\api\scripts\run_gls_staging_pipeline.py" %*
