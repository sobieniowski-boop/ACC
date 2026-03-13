@echo off
setlocal
cd /d C:\ACC\apps\api
"C:\ACC\.venv\Scripts\python.exe" "C:\ACC\apps\api\scripts\run_gls_pipeline_background.py" --invoice-root "N:\KURIERZY\GLS POLSKA" --bl-map-path "N:\KURIERZY\GLS POLSKA\GLS - BL.xlsx" --limit-shipments 200000 --limit-orders 300000 --include-shipment-seed
