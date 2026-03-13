"""Insert _sync_search_terms scheduler function before _seasonality_build_monthly."""
with open(r"C:\ACC\apps\api\app\scheduler.py", "r", encoding="utf-8") as f:
    content = f.read()

new_func = '''
async def _sync_search_terms():
    """Weekly Wed 03:00 \u2014 Sync Brand Analytics search term reports."""
    job_id = _create_job_record("sync_search_terms")
    log.info("scheduler.search_terms.start", job_id=job_id)
    try:
        from app.services.search_term_sync import sync_search_terms

        result = await sync_search_terms(weeks_back=4)
        set_job_success(
            job_id,
            records_processed=result.get("total_weekly_rows", 0),
            message=f"weekly={result.get('total_weekly_rows',0)} monthly={result.get('monthly_rows',0)}",
        )
        log.info("scheduler.search_terms.done", result=result)
    except Exception as exc:
        log.error("scheduler.search_terms.error", error=str(exc))
        set_job_failure(job_id, str(exc))


'''

target = "async def _seasonality_build_monthly():"
pos = content.find(target)
if pos < 0:
    print("TARGET NOT FOUND")
else:
    content = content[:pos] + new_func + content[pos:]
    with open(r"C:\ACC\apps\api\app\scheduler.py", "w", encoding="utf-8") as f:
        f.write(content)
    print(f"INSERTED at character position {pos}")
