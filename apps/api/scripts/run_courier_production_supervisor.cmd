@echo off
setlocal
cd /d C:\ACC\apps\api

set LOG_FILE=C:\ACC\apps\api\scripts\courier_prod_supervisor.log
set CHECKPOINT_FILE=C:\ACC\apps\api\scripts\courier_prod_supervisor_checkpoint.json

echo [%DATE% %TIME%] START courier production supervisor >> "%LOG_FILE%"
"C:\Users\msobieniowski\AppData\Local\Programs\Python\Python312\python.exe" ^
  scripts\run_courier_order_universe_supervisor.py ^
  --months 2025-11 2025-12 2026-01 ^
  --carriers DHL GLS ^
  --limit-orders 3000000 ^
  --stale-timeout-sec 900 ^
  --hard-timeout-sec 7200 ^
  --transient-retries 2 ^
  --checkpoint-file "%CHECKPOINT_FILE%" >> "%LOG_FILE%" 2>&1

echo [%DATE% %TIME%] END courier production supervisor >> "%LOG_FILE%"
endlocal
