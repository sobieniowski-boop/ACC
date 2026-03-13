"""P0 Production baseline snapshot — gather schema + event metrics."""
import sys, os, hashlib, json
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "apps", "api"))
os.chdir(os.path.join(os.path.dirname(__file__), "apps", "api"))

from app.core.db_connection import connect_acc

conn = connect_acc(autocommit=True)
cur = conn.cursor()

results = {}

# ── 1. TABLES ──
cur.execute("""
SELECT t.name AS table_name, SUM(p.rows) AS row_count
FROM sys.tables t
JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
GROUP BY t.name
ORDER BY t.name
""")
tables = [(r[0], r[1]) for r in cur.fetchall()]
results["tables"] = tables
print(f"Tables: {len(tables)}")

# ── 2. INDEXES ──
cur.execute("""
SELECT t.name, i.name, i.type_desc, i.is_unique, i.is_primary_key
FROM sys.indexes i
JOIN sys.tables t ON i.object_id = t.object_id
WHERE i.name IS NOT NULL
ORDER BY t.name, i.name
""")
indexes = [(r[0], r[1], r[2], r[3], r[4]) for r in cur.fetchall()]
results["indexes"] = indexes
print(f"Indexes: {len(indexes)}")

# ── 3. CONSTRAINTS ──
cur.execute("""
SELECT t.name, c.name, c.type_desc
FROM sys.objects c
JOIN sys.tables t ON c.parent_object_id = t.object_id
WHERE c.type IN ('PK','UQ','F','C','D')
ORDER BY t.name, c.type_desc, c.name
""")
constraints = [(r[0], r[1], r[2]) for r in cur.fetchall()]
results["constraints"] = constraints
print(f"Constraints: {len(constraints)}")

# ── 4. SCHEMA CHECKSUM ──
schema_blob = json.dumps({
    "tables": [(t[0], int(t[1])) for t in tables],
    "indexes": [(i[0], i[1], i[2]) for i in indexes],
    "constraints": [(c[0], c[1], c[2]) for c in constraints],
}, sort_keys=True)
checksum = hashlib.sha256(schema_blob.encode()).hexdigest()[:16]
print(f"Schema checksum: {checksum}")

# ── 5. EVENT BACKBONE METRICS ──
event_metrics = {}

# Check if event tables exist
cur.execute("SELECT OBJECT_ID('dbo.acc_event_log', 'U')")
event_log_exists = cur.fetchone()[0] is not None

cur.execute("SELECT OBJECT_ID('dbo.acc_event_processing_log', 'U')")
proc_log_exists = cur.fetchone()[0] is not None

if event_log_exists:
    cur.execute("SELECT COUNT(*) FROM acc_event_log WITH (NOLOCK)")
    event_metrics["event_log_total"] = cur.fetchone()[0]

    cur.execute("SELECT status, COUNT(*) FROM acc_event_log WITH (NOLOCK) GROUP BY status")
    event_metrics["event_log_by_status"] = {r[0]: r[1] for r in cur.fetchall()}
else:
    event_metrics["event_log_total"] = "TABLE_NOT_FOUND"
    event_metrics["event_log_by_status"] = {}

if proc_log_exists:
    cur.execute("SELECT COUNT(*) FROM acc_event_processing_log WITH (NOLOCK)")
    event_metrics["processing_log_total"] = cur.fetchone()[0]
else:
    event_metrics["processing_log_total"] = "TABLE_NOT_FOUND"

print(f"Event log total: {event_metrics['event_log_total']}")
print(f"Event log by status: {event_metrics['event_log_by_status']}")
print(f"Processing log total: {event_metrics['processing_log_total']}")

# ── 6. STUCK EVENTS ──
stuck_events = []
if event_log_exists:
    cur.execute("""
    SELECT id, event_id, notification_type, event_domain, status, received_at, retry_count, error_message
    FROM acc_event_log WITH (NOLOCK)
    WHERE status = 'processing'
      AND received_at < DATEADD(minute, -10, GETUTCDATE())
    """)
    cols = [d[0] for d in cur.description]
    for row in cur.fetchall():
        stuck_events.append(dict(zip(cols, row)))

print(f"Stuck events: {len(stuck_events)}")

cur.close()
conn.close()

# ── 7. SQS STATUS ──
from app.core.config import settings
sqs_status = {}
sqs_url = settings.SQS_QUEUE_URL
sqs_status["queue_url"] = sqs_url or "(not configured)"
sqs_status["region"] = settings.SQS_REGION

if sqs_url:
    try:
        import boto3
        session_kwargs = {"region_name": settings.SQS_REGION}
        if settings.AWS_ACCESS_KEY_ID:
            session_kwargs["aws_access_key_id"] = settings.AWS_ACCESS_KEY_ID
            session_kwargs["aws_secret_access_key"] = settings.AWS_SECRET_ACCESS_KEY
        sqs = boto3.client("sqs", **session_kwargs)
        attrs = sqs.get_queue_attributes(
            QueueUrl=sqs_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"]
        )["Attributes"]
        sqs_status["approx_messages"] = int(attrs.get("ApproximateNumberOfMessages", 0))
        sqs_status["approx_not_visible"] = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        sqs_status["approx_delayed"] = int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
        sqs_status["status"] = "ok"
    except ImportError:
        sqs_status["status"] = "boto3_not_installed"
        sqs_status["approx_messages"] = "N/A"
    except Exception as e:
        sqs_status["status"] = f"error: {e}"
        sqs_status["approx_messages"] = "N/A"
else:
    sqs_status["status"] = "not_configured"
    sqs_status["approx_messages"] = "N/A"

print(f"SQS status: {sqs_status['status']}, msgs: {sqs_status['approx_messages']}")

# ── SAVE ALL TO JSON for report generation ──
output = {
    "timestamp": datetime.utcnow().isoformat() + "Z",
    "checksum": checksum,
    "tables": [(t[0], int(t[1])) for t in tables],
    "indexes": [(i[0], i[1], i[2], bool(i[3]), bool(i[4])) for i in indexes],
    "constraints": [(c[0], c[1], c[2]) for c in constraints],
    "event_metrics": event_metrics,
    "stuck_events": [
        {k: str(v) for k, v in evt.items()} for evt in stuck_events
    ],
    "sqs": sqs_status,
}

out_path = os.path.join(os.path.dirname(__file__), "_p0_snapshot_data.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(output, f, indent=2, default=str, ensure_ascii=False)

print(f"\nSnapshot data saved to {out_path}")
