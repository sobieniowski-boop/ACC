"""Fix ads scheduler: change from daily 07:00 cron to 4h interval."""
import re

with open(r"c:\ACC\apps\api\app\scheduler.py", "r", encoding="utf-8") as f:
    content = f.read()

# Fix 1: _sync_ads docstring
content = content.replace(
    '"""07:00',
    '"""Every 4h',
    1,  # only first occurrence (which is in _sync_ads)
)

# Fix 2: scheduler job definition — find the ads job block and replace
# The pattern: CronTrigger(hour=7, minute=0) → IntervalTrigger(hours=4)
# and id/name changes
old_trigger = 'trigger=CronTrigger(hour=7, minute=0),'
new_trigger = 'trigger=IntervalTrigger(hours=4),'
content = content.replace(old_trigger, new_trigger, 1)

old_id = 'id="sync-ads-daily",'
new_id = 'id="sync-ads-4h",'
content = content.replace(old_id, new_id, 1)

old_name = 'name="Sync Amazon Ads (07:00)",'
new_name = 'name="Sync Amazon Ads (every 4h)",'
content = content.replace(old_name, new_name, 1)

# Fix the comment line above the job (find by "# 07:00" before "Amazon Ads sync")
content = re.sub(
    r'# 07:00 .+ Amazon Ads sync \(profiles .+ campaigns .+ daily reports\)',
    '# Every 4h — Amazon Ads sync (profiles → campaigns → daily reports)',
    content,
    count=1,
)

with open(r"c:\ACC\apps\api\app\scheduler.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Done. Verifying...")

# Verify
with open(r"c:\ACC\apps\api\app\scheduler.py", "r", encoding="utf-8") as f:
    content = f.read()

assert 'IntervalTrigger(hours=4)' in content, "IntervalTrigger not found!"
assert 'sync-ads-4h' in content, "New job ID not found!"
assert 'every 4h' in content.lower(), "New name not found!"
assert 'CronTrigger(hour=7, minute=0)' not in content or content.count('CronTrigger(hour=7, minute=0)') == content.count('CronTrigger(hour=7, minute=0)'), "Check"
print("All assertions passed.")
