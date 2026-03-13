"""Insert search-terms scheduler job registration before seasonality-build-monthly-daily."""
with open(r"C:\ACC\apps\api\app\scheduler.py", "r", encoding="utf-8") as f:
    content = f.read()

# Find the line that registers seasonality-build-monthly-daily
target = 'id="seasonality-build-monthly-daily"'
pos = content.find(target)
if pos < 0:
    print("TARGET NOT FOUND")
    raise SystemExit(1)

# Go back to find the comment line before this block
# The block starts with "# Daily 04:30"
block_start_text = "Seasonality: build monthly aggregates"
# Search backwards from pos
search_area = content[:pos]
comment_pos = search_area.rfind(block_start_text)
if comment_pos < 0:
    print("COMMENT NOT FOUND")
    raise SystemExit(1)

# Go back to the start of the comment line (find the preceding newline + whitespace + #)
line_start = search_area.rfind("\n", 0, comment_pos)
if line_start < 0:
    line_start = 0
else:
    line_start += 1  # skip the newline itself

new_job = '''    # Weekly Wednesday 03:00 \u2014 Search Terms: sync Brand Analytics data
    scheduler.add_job(
        _sync_search_terms,
        trigger=CronTrigger(day_of_week="wed", hour=3, minute=0),
        id="sync-search-terms-weekly",
        name="Sync Search Terms (Wed 03:00)",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=1200,
    )

'''

content = content[:line_start] + new_job + content[line_start:]
with open(r"C:\ACC\apps\api\app\scheduler.py", "w", encoding="utf-8") as f:
    f.write(content)
print(f"INSERTED scheduler job at position {line_start}")
