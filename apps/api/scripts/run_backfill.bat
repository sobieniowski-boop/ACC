@echo off
cd /d C:\ACC\apps\api
echo Starting backfill at %DATE% %TIME% >> backfill_launcher.log
python scripts\backfill_orders.py >> backfill_stdout.log 2>> backfill_stderr.log
echo Finished at %DATE% %TIME% >> backfill_launcher.log
