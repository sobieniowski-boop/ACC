"""Patch scheduler.py to add SQS poll jobs and optimize frequencies."""
import re

with open(r"c:\ACC\apps\api\app\scheduler.py", "r", encoding="utf-8") as f:
    content = f.read()

# 1) Insert two new job functions before the "Public API" section
new_functions = '''
async def _poll_sqs_notifications():
    """Every 2 min - poll SQS for SP-API notifications -> ingest into Event Backbone."""
    job_id = _create_job_record("poll_sqs_notifications")
    log.info("scheduler.poll_sqs.start", job_id=job_id)
    try:
        import asyncio
        from app.services.event_backbone import poll_sqs

        result = await asyncio.to_thread(poll_sqs, max_messages=10)
        received = result.get("received", 0)
        set_job_success(job_id, records_processed=received, message=f"received={received}")
        if received:
            log.info("scheduler.poll_sqs.done", received=received)
    except Exception as exc:
        log.error("scheduler.poll_sqs.error", error=str(exc))
        set_job_failure(job_id, str(exc))


async def _process_notification_events():
    """Every 5 min - process pending events from the Event Backbone."""
    job_id = _create_job_record("process_notification_events")
    log.info("scheduler.process_events.start", job_id=job_id)
    try:
        import asyncio
        from app.services.event_backbone import process_pending_events

        result = await asyncio.to_thread(process_pending_events, limit=100)
        processed = result.get("processed", 0)
        set_job_success(job_id, records_processed=processed, message=f"processed={processed}")
        if processed:
            log.info("scheduler.process_events.done", processed=processed)
    except Exception as exc:
        log.error("scheduler.process_events.error", error=str(exc))
        set_job_failure(job_id, str(exc))

'''

# Find the "Public API" comment block
marker = "# Public API"
idx = content.find(marker)
if idx == -1:
    raise ValueError("Could not find 'Public API' marker")

# Go back to find the start of the comment block (the dashes line before it)
# Find the "# ---" line before "Public API"
dash_line_start = content.rfind("\n# ----", 0, idx)
if dash_line_start == -1:
    raise ValueError("Could not find dash line before Public API")

# Insert new functions before the dash line
content = content[:dash_line_start] + "\n" + new_functions + content[dash_line_start:]

# 2) Add scheduler.add_job() calls for the two new jobs right before scheduler.start()
new_jobs = '''
    # Every 2 min - poll SQS for SP-API real-time notifications
    scheduler.add_job(
        _poll_sqs_notifications,
        trigger=IntervalTrigger(minutes=2),
        id="poll-sqs-notifications-2m",
        name="Poll SQS Notifications (2 min)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60,
    )

    # Every 5 min - process pending events from Event Backbone
    scheduler.add_job(
        _process_notification_events,
        trigger=IntervalTrigger(minutes=5),
        id="process-notification-events-5m",
        name="Process Notification Events (5 min)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=120,
    )

'''

# Insert before scheduler.start()
start_marker = "    scheduler.start()"
idx2 = content.find(start_marker)
if idx2 == -1:
    raise ValueError("Could not find scheduler.start()")
content = content[:idx2] + new_jobs + content[idx2:]

# 3) Optimize: order-pipeline 15m -> 30m (ORDER_STATUS_CHANGE is now real-time)
content = content.replace(
    'trigger=IntervalTrigger(minutes=15),\n        id="order-pipeline-15m",\n        name="Order Pipeline (15 min)",',
    'trigger=IntervalTrigger(minutes=30),\n        id="order-pipeline-30m",\n        name="Order Pipeline (30 min, real-time via SQS)",',
)

# 4) Optimize: sync-fba-inventory 4h -> 8h (FBA_INVENTORY_AVAILABILITY_CHANGES is now real-time)
content = content.replace(
    'trigger=IntervalTrigger(hours=4),\n        id="sync-fba-inventory-4h",\n        name="Sync FBA Inventory (4h)",',
    'trigger=IntervalTrigger(hours=8),\n        id="sync-fba-inventory-8h",\n        name="Sync FBA Inventory (8h, real-time via SQS)",',
)

with open(r"c:\ACC\apps\api\app\scheduler.py", "w", encoding="utf-8") as f:
    f.write(content)

print("DONE - scheduler.py patched successfully")
print("  + Added _poll_sqs_notifications() function")
print("  + Added _process_notification_events() function")
print("  + Added scheduler.add_job() for both")
print("  + Order pipeline: 15m -> 30m")
print("  + FBA inventory: 4h -> 8h")
