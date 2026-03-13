# Event Backbone — Dead-Letter Monitoring & Alerting (P2)

## Overview

P2 extends the Event Backbone with automated dead-letter detection,
threshold-based alerting, and a dedicated health API.

| Component | File |
|---|---|
| Monitoring service | `app/services/guardrails_backbone.py` |
| HTTP API | `app/api/v1/backbone.py` |
| Migration | `migrations/versions/20260310_system_alert.py` |
| Alert table | `dbo.acc_system_alert` |

## Architecture

```
┌──────────────────────────────────────┐
│         APScheduler (5 min)          │
│  evaluate_and_alert()                │
└──────────┬───────────────────────────┘
           │
           ▼
┌──────────────────────────────────────┐
│  guardrails_backbone.py              │
│                                      │
│  check_backbone_dead_letters()       │  ← failed events in last hour
│  check_backbone_pending_depth()      │  ← 'received' queue depth
│  check_backbone_processing_rate()    │  ← throughput metric
│                                      │
│  evaluate_and_alert()                │  ← orchestrates all checks
│    └─ send_backbone_alert()          │  ← CRITICAL → acc_system_alert
│                                      │
│  get_backbone_health_summary()       │  ← API health endpoint
└──────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────┐
│  acc_system_alert (Azure SQL)        │
│  id, alert_type, severity,           │
│  message, details, created_at        │
└──────────────────────────────────────┘
```

## Thresholds

| Check | Warning | Critical |
|---|---|---|
| Dead letters (last hour) | >0 | >5 |
| Pending depth | ≥500 | ≥2000 |
| Processing rate | — (informational) | — |

When dead letters exceed the CRITICAL threshold (5), `send_backbone_alert()`
fires automatically — logging at CRITICAL level and persisting a row in
`acc_system_alert`.

## API Endpoints

All under prefix `/api/v1/backbone`.

### `GET /backbone/health`

Returns aggregated health snapshot:

```json
{
  "status": "healthy",
  "failed_last_hour": 0,
  "pending_events": 12,
  "processing_rate": 340,
  "alert_threshold": 5,
  "open_circuits": [],
  "circuit_breakers": [],
  "elapsed_ms": 45.2
}
```

`status` values: `healthy` | `degraded` | `critical`.

### `POST /backbone/evaluate`

Trigger an immediate evaluation cycle. Returns guardrail results and fires
alerts if thresholds are breached.

### `GET /backbone/alerts?limit=50&alert_type=event_backbone_failure`

Query recent alerts from `acc_system_alert`.

## Scheduler Integration

Add to the APScheduler job list (e.g. in `app/scheduler.py`):

```python
from app.services.guardrails_backbone import evaluate_and_alert

scheduler.add_job(
    evaluate_and_alert,
    "interval",
    minutes=5,
    id="backbone_dead_letter_monitor",
    name="Backbone dead-letter monitor",
    replace_existing=True,
)
```

## Relationship to P1

P2 builds on the P1 circuit breaker. The health endpoint merges:

- **Dead-letter metrics** from `guardrails_backbone.py`
- **Circuit breaker state** from `event_backbone.get_handler_health()`

If any circuit breaker is open OR dead letters exceed threshold,
`status` reports `"critical"`.

## Migration

Revision `eb003` (depends on `eb002` from P1):

```sql
CREATE TABLE dbo.acc_system_alert (
    id          BIGINT IDENTITY(1,1) PRIMARY KEY,
    alert_type  VARCHAR(100)   NOT NULL,
    severity    VARCHAR(20)    NOT NULL,
    message     NVARCHAR(2000) NOT NULL,
    details     NVARCHAR(MAX)  NULL,
    created_at  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    INDEX ix_system_alert_type_created (alert_type, created_at)
)
```

Apply via Alembic: `alembic upgrade eb003`

Or auto-created on first alert by `send_backbone_alert()` (IF NOT EXISTS pattern).
